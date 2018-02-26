const verify = require('@octokit/webhooks/verify')
const config = require('./config.json');
const octokit = require('@octokit/rest')();

octokit.authenticate({
  type: 'basic',
  username: '',
  password: ''
})

/**
 * Responds to any HTTP request that can provide a "message" field in the body.
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
  });

  if (!verifyGitHubRequst(req)) {
    res.status(400);
  }

  const request_event_type = req.headers['x-github-event'];

  console.log(`request_event_type: ${request_event_type}`);

  switch(request_event_type) {
    case 'push':
      console.log(JSON.stringify(req.body, null, 4));
      break;
    case 'ping':
      res.status(200).send('OK');
      break;
    default:
      console.log(`Unknown request event type: ${request_event_type}`)
      res.status(400);
  }
};

// console.log(JSON.stringify(req.body, null, 4));

// Example input: {"message": "Hello!"}
// if (req.body.message === undefined) {
//   // This is an error case, as "message" is required.
//   res.status(400).send('hoge');
// } else {
//   // Everything is okay.
//   console.log(req.body.message);
// res.status(200).send('OK');
  // res.status(200).send('Success: ' + req.body.message);
// }


/**
 * verify request using HMAC-SHA1 header: `X-Hub-Signature: sha1=deadbeaf....`
 *
 * @param {!Object} req Clound Function request context.
 * @return Boolean whether verification of request succeed.
 */
function verifyGitHubRequst(req) {
  const header_str = req.headers['x-hub-signature'];
  if (!header_str) {
    console.error('Bad request, not supplied X-Hub-Signature header');
    return false;
  }

  return verify(config.githubSecret, req.rawBody, header_str)
};
