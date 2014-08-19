module.exports = function getVersionParts (version) {
    var base = version.split(/[A-Z]|[a-z]|-/)[0];
    var versionParts = base.split('.');
    var labelRegExp = new RegExp("^" + base);
    var label = version.replace(labelRegExp, "");
    var isStable = (!label);
    return {
        version: version,
        base: base,
        major: parseInt(versionParts[0]),
        minor: parseInt(versionParts[1]),
        patch: parseInt(versionParts[2]),
        label: label,
        isStable: isStable
    };
}