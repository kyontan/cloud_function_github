const verify = require('@octokit/webhooks/verify');
const config = require('./config.json');
const octokit = require('@octokit/rest')();
const BigQuery = require('@google-cloud/bigquery');

process.on('unhandledRejection', console.error);

let isEnumSourceFile = git_tree_obj =>
  git_tree_obj.type === 'blob' && git_tree_obj.path.match('enums/.*.java');


let getPlainContent = get_content_res =>
  Buffer.from(get_content_res.data.content, 'base64').toString();

let camelToSnakeCase = str =>
  str
    .split(/(?=[A-Z])/)
    .join('_')
    .toLowerCase();

/**
 * Extract enum key, code, and values from Java Source Code
 * @params source_code Java Source code of enum
 * @return { name: EnumName, values: ([[ key, (code, value) ], ...] or null) } or null
 */
let parseEnumDeclaration = source_code => {
  // matches "enum...; (first semi-colon that indicates end of enum value declaration, allowed multi line)
  const re_whole_enum = / enum ([^\s]+).*?((?:\n.*?)*?);/m;

  // matches upto 3 literals inside parenthes:
  // ex. KEY(CODE, FIRST_LABEL, SECOND_LABEL) can be matched with [CODE, FIRST_LABEL, SECOND_LABEL]
  // ex. KEY(CODE, FIRST_LABEL, SECOND_LABEL, THIRD_LABEL) can be matched with [CODE, FIRST_LABEL, SECOND_LABEL], more than 2 labels to be ignored.
  const re_key = `([A-Z_][A-Z0-9_]*)`;
  const re_literal = `(?:(true|false|[0-9]+(?:\\.[0-9])?)|(?:'(.*?)'|"(.*?)"))`;
  const re_comma = `\\s*,\\s*`;
  const re_code = re_literal;
  const re_label = `(?:${re_comma}${re_literal})`;
  const re_test_enum_with_paren = new RegExp(`${re_key}\\s*\\(.*\\)`, 'g'); // for testing whether enum has parentheses
  const re_enum_with_paren = new RegExp(
    `${re_key}\\s*\\(\\s*${re_code}${re_label}?${re_label}?${re_label}*\\s*\\)`,
    'g'
  );
  const re_enum_without_paren = new RegExp(re_key, 'g'); // only key

  const whole_matched = re_whole_enum.exec(source_code);
  if (!whole_matched) return null;

  const enum_name = whole_matched[1]; // ex. SomeEnum
  const enum_value_section = whole_matched[2]; // ex. '\n    HOGE(0, "some"),\n    FUGA(1, "simple"),\n    NYAN(2, "str")'

  let m,
    matched = [];

  let re_enum,
    is_autoindex_code = false,
    autoindex_code = 0;
  if (re_test_enum_with_paren.test(enum_value_section)) {
    // enum SomeEnum { Hoge(...), Fuga(...), Nyan(...); }
    re_enum = re_enum_with_paren;
  } else {
    // enum SomeEnum { Hoge, Fuga, Nyan; }
    is_autoindex_code = true;
    re_enum = re_enum_without_paren;
  }

  while ((m = re_enum.exec(enum_value_section))) {
    // [whole_matched, 1st, 2nd, 3rd] (all elements can be `undefined`)
    // `filter(x => x)` removes undefined, null
    if (is_autoindex_code) {
      matched.push([m[1], autoindex_code++]);
    } else {
      matched.push(m.slice(1).filter(x => x));
    }
  }

  return { name: enum_name, values: matched };
};

/**
 * Guess schema matches to rows of data.
 * @returns [string]
 * @example
 *
 * getTypes([["something", "123"], ["string", "123"]])
 * // [ 'string', 'integer' ]
 *
 * getTypes([["something", "123", "true"], ["string", "123", "false"]])
 * // [ 'string', 'integer', 'boolean' ]
 *
 * getTypes([["123"], ["not_number"]])
 * // [ 'string' ]
 *
 * getTypes([["123"], ["123"]])
 * // [ 'integer' ]
 *
 * getTypes([["true"], ["false"]])
 * // [ 'boolean' ]
 *
 * getTypes([["1st column"], ["1st", "2nd"]])
 * // [ 'string', 'string' ]
 */
let guessSchema = rows => {
  let parseIntIfParsable = str =>
    /^[0-9]+$/.test(str) ? parseInt(str, 10) : str;

  // transpose 2d array
  // @note this transposes all columns as many as possible.
  //       transpose([[0, 1, 2], [0, 1]] // returns [[0, 0], [1, 1], [2, undefined]]
  let transpose = a => {
    let cols = Math.max(...a.map(x => x.length)),
      ret = [];
    for (let c = 0; c < cols; c++) {
      ret[c] = a.map(r => r[c]);
    }
    return ret;
  };

  let isBoolean = x => x === 'true' || x === 'false';
  let isNumber = x => typeof x === 'number';

  return transpose(rows)
    .filter(x => x)
    .map(x => {
      if (x.every(y => isBoolean(y))) return 'boolean';
      if (x.every(y => isNumber(parseIntIfParsable(y)))) return 'integer';
      return 'string';
    });
};

let getAllEnumSourceFromRepository = (repo, owner, commit_sha) => {
  octokit.authenticate(config.octokitAuthenticateOption);
  return octokit.gitdata
    .getTree({ owner: owner, repo: repo, sha: commit_sha, recursive: true })
    .then(res_tree_ary => res_tree_ary.data.tree.filter(isEnumSourceFile))
    .then(git_tree_ary =>
      Promise.all(
        git_tree_ary.map(git_tree_obj =>
          octokit.repos.getContent({
            owner: owner,
            repo: repo,
            path: git_tree_obj.path,
            sha: commit_sha,
          })
        )
      )
    )
    .then(contents => contents.map(x => getPlainContent(x)));
};

// console.log(JSON.stringify(req.body, null, 4));

/**
 * verify request using HMAC-SHA1 header: `X-Hub-Signature: sha1=deadbeaf....`
 *
 * @param {!Object} req Clound Function request context.
 * @return Boolean whether verification of request succeed.
 */
let verifyGitHubRequst = req => {
  const header_str = req.headers['x-hub-signature'];
  if (!header_str) {
    console.error('Bad request, not supplied X-Hub-Signature header');
    return false;
  }

  return verify(config.githubWebhookSecret, req.rawBody, header_str);
};

/**
 * Insert rows to BigQuery
 * @note if table `tableId` exists, it will be deleted (overwritten)
 * @note create dataset and table if not exists.
 *
 * @param projectId string
 * @param datasetId string
 * @param tableId string
 * @param tableSchemaFields [Object]
 *   Object should have key `name` and `type`
 *   Matches schema to `schema` of https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#resource
 * @param rows [Object]
 *
 * @example
 * const datasetId = "...";
 * const tableId = "...";
 * const tableSchemaFields = [
 *   { "name": "key", "type": "string" },
 *   { "name": "code", "type": "integer" },
 *   { "name": "label", "type": "string" },
 * ];
 * const rows = [
 *   { "key": "SOME_DAY", "code": "1", "label": "It's someday" },
 *   { "key": "EVERY_DAY", "code": "42", "label": "Everyday" },
 *   { "key": "HAPPY_DAY", "code": "999", "label": "Maybe happy" },
 * ];
 * insertRowsToBigQuery(datasetId, tableId, tableSchemaFields, rows);
 */
let insertRowsToBigQuery = (
  projectId,
  datasetId,
  tableId,
  tableSchemaFields,
  rows
) => {
  const bigquery = new BigQuery({ projectId: projectId });

  // Create a new table in the dataset
  return bigquery
    .dataset(datasetId)
    .get({ autoCreate: true })
    .then(([dataset]) => {
      let table = dataset.table(tableId);
      return table
        .exists()
        .then(([isExists]) => {
          let res = Promise.resolve();
          if (isExists)
            res = table.delete();

          return res.then(() => table.get({ schema: { fields: tableSchemaFields }, autoCreate: true })); // create table
        })
    })
    .then(([table]) => table.insert(rows))
    .then(() => {
      console.log(`Inserted ${rows.length} rows into ${datasetId}.${tableId}`);
    })
    .catch(err => {
      if (err && err.name === 'PartialFailureError') {
        if (err.errors && err.errors.length > 0) {
          console.log('Insert errors:');
          err.errors.forEach(err => console.error(err));
        }
      } else {
        console.error('ERROR:', err);
      }
    });
};

/**
 * Responds to any HTTP request that can handle GitHub webhook's push event
 *
 * @param {!Object} req Cloud Function request context.
 * @param {!Object} res Cloud Function response context.
 */
exports.github = (req, res) => {
  console.log({
    params: req.params,
    query: req.query,
    url: req.url,
    method: req.method,
    baseUrl: req.baseUrl,
    _parsedUrl: req._parsedUrl,
    headers: req.headers,
    body: req.body,
  });

  if (!verifyGitHubRequst(req)) {
    return res.status(400);
  }

  const request_event_type = req.headers['x-github-event'];

  console.log(`request_event_type: ${request_event_type}`);

  if (request_event_type === 'push') {
    // console.log(JSON.stringify(req.body, null, 4));
    let repo = req.body.repository.name;
    let owner = req.body.repository.owner.name;
    let commit_sha = req.body.head_commit.id;

    console.log({ repo: repo, owner: owner, commit_sha: commit_sha });

    return getAllEnumSourceFromRepository(repo, owner, commit_sha)
      .then(contents =>
        contents
          .map(parseEnumDeclaration)
          .filter(x => x && x.values && 0 < x.values.length) // remove not matched
          .map(x => {
            let table_id = camelToSnakeCase(x.name);

            let columns = ['key', 'code', 'label'];
            // takes upto 3 elements (that equals to what `columns` has)
            let field_types = guessSchema(x.values).slice(0, 3);

            let bq_schema_fields = field_types.map((type, i) => ({
              name: columns[i],
              type: type,
            }));

            let rows = x.values.map(values => {
              let row = {};
              field_types.map((_, i) => (row[columns[i]] = values[i]));
              return row;
            });

            return insertRowsToBigQuery(
              config.gcpProjectId,
              config.gcpDatasetId,
              table_id,
              bq_schema_fields,
              rows
            );
          })
      )
      .then(() => res.status(200).send('OK'))
      .catch(err => {
        console.error(err.stack);
        res.status(500).send(err.message);
      });
  } else if (request_event_type === 'ping') {
    return res.status(200).send('OK');
  } else {
    console.log(`Unknown request event type: ${request_event_type}`);
    return res.status(400);
  }
};
