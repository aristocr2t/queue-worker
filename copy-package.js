const fs = require('fs');
const packageJson = JSON.parse(fs.readFileSync('package.json').toString());

delete packageJson.scripts;

fs.writeFileSync('dist/package.json', JSON.stringify(packageJson, null, 2));
