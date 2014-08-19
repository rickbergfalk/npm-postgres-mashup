var assert = require('assert');
var getVersionParts = require('./get-version-parts.js');

var v1 = getVersionParts('1.2.3ALPHA-4.5.6');
assert.equal(v1.major, 1);
assert.equal(v1.minor, 2);
assert.equal(v1.patch, 3);
assert.equal(v1.base, '1.2.3');
assert.equal(v1.label, 'ALPHA-4.5.6');
assert.equal(v1.isStable, false);

var v2 = getVersionParts('1.2.3');
assert.equal(v2.major, 1);
assert.equal(v2.minor, 2);
assert.equal(v2.patch, 3);
assert.equal(v2.base, '1.2.3');
assert.equal(v2.label, '');
assert.equal(v2.isStable, true);

var v3 = getVersionParts('1.2.3-RELEASECANDIDATE');
assert.equal(v3.major, 1);
assert.equal(v3.minor, 2);
assert.equal(v3.patch, 3);
assert.equal(v3.base, '1.2.3');
assert.equal(v3.label, '-RELEASECANDIDATE');
assert.equal(v3.isStable, false);
