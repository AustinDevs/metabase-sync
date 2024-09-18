const {
  writeFileSync, readdirSync, statSync, readFileSync, unlinkSync, cpSync, existsSync, mkdirSync, rmSync
} = require('fs');
const tar = require('tar');
const path = require('path');
const yaml = require('js-yaml');
const { nanoid: Nanoid } = require('nanoid');
const request = require('request-promise-native');
const dictionary = new Set(readFileSync(require('word-list'), 'utf-8').split('\n').filter((word) => word.length >= 5));
const config = require('./metabase-sync.config.json');
const { METABASE_API_KEY, METABASE_URL, FROM_DATABASE_ID, TO_DATABASE_ID } = process.env;
const SERIALIZATION_API_URL = `${METABASE_URL}/api/ee/serialization`;

const exportCollection = async (databaseId) => {
  const collectionId = config.collections.find((collection) => collection.database_id === databaseId)?.collection_id;
  if (!collectionId) {
    console.error(`Collection ID not found for database: ${databaseId}`);
    return;
  }
  const dirname = 'from_collection';
  const buffer = Buffer.from(await (await fetch(`${SERIALIZATION_API_URL}/export?settings=false&data_model=false&collection=${collectionId}&dirname=${dirname}`, {
    method: 'POST',
    headers: { 'x-api-key': METABASE_API_KEY }
  })).arrayBuffer());
  const cwd = path.join(__dirname, 'tmp');
  const directory = path.join(__dirname, 'tmp', 'from_collection');
  const file = `${directory}.tar.gz`;
  writeFileSync(file, buffer);
  await tar.x({ file, cwd });
  cpSync(path.join(__dirname, 'tmp', 'from_collection'), path.join(__dirname, 'tmp', 'to_collection'), { recursive: true });
  return path.join(__dirname, 'tmp', 'to_collection');
}

function detectWordsInString(str) {
  str = str.toLowerCase();
  const foundWords = [];
  const strLength = str.length;
  for (let i = 0; i < strLength; i++) {
    for (let j = i + 1; j <= strLength; j++) {
      const substring = str.slice(i, j);
      if (dictionary.has(substring)) {
        foundWords.push(substring);
      }
    }
  }
  return foundWords;
}

function isValidNanoid(nanoid) {
  const hasMixedCase = /[A-Z]/.test(nanoid) && /[a-z]/.test(nanoid);
  const hasNumbers = /[0-9]/.test(nanoid);
  const noLongConsecutive = !/(.)\1{3,}/.test(nanoid);
  const noWords = detectWordsInString(nanoid).length === 0;
  const isNanoid = nanoid.length === 21 && hasMixedCase && noLongConsecutive && (hasNumbers || noWords);
  if (!isNanoid) {
    console.log(`Invalid nanoid: ${nanoid}`);
    return false;
  }
  console.log(`Valid nanoid: ${nanoid}`);
  return true;
}

function findNanoidsInFile(filePath) {
  const content = readFileSync(filePath, 'utf-8');
  const nanoidRegex = /[A-Za-z0-9_-]{21}/g;
  let nanoids = content.match(nanoidRegex);
  nanoids = nanoids.filter(isValidNanoid);
  nanoids = Array.from(new Set(nanoids));
  return nanoids || [];
}

function findNanoidsInDirectory(dirPath) {
  let allNanoids = [];
  const files = readdirSync(dirPath);
  files.forEach(file => {
    const filePath = path.join(dirPath, file);
    if (statSync(filePath).isDirectory()) {
      allNanoids = allNanoids.concat(findNanoidsInDirectory(filePath));
    } else if (statSync(filePath).isFile()) {
      const nanoids = findNanoidsInFile(filePath);
      if (nanoids.length > 0) {
        allNanoids = allNanoids.concat(nanoids);
      }
    }
  });
  return allNanoids;
}

function replaceStringInDirectory(rootDir, targetString, replacementString) {
  const files = readdirSync(rootDir);
  for (const file of files) {
    const filePath = path.join(rootDir, file);
    const stats = statSync(filePath);
    if (stats.isDirectory()) {
      replaceStringInDirectory(filePath, targetString, replacementString);
    } else if (stats.isFile()) {
      const data = readFileSync(filePath, 'utf8');
      const newData = data.replace(new RegExp(targetString, 'g'), replacementString);
      if (data !== newData) {
        writeFileSync(filePath, newData, 'utf8');
      }
    }
  }
}

async function importCollection(directory) {
  directory = directory.replace('from_collection', 'to_collection');
  const filePath = `${directory}.tar.gz`;
  process.chdir('tmp');
  await tar.create({ gzip: true, file: filePath }, ['to_collection/']);
  const buffer = readFileSync(filePath);
  const response = await request({
    method: 'POST',
    uri: `${SERIALIZATION_API_URL}/import`,
    headers: {
      'x-api-key': METABASE_API_KEY,
      'Content-Type': 'multipart/form-data',
    },
    formData: {
      file: {
        value: buffer,
        options: {
          filename: 'file.tar.gz',
          contentType: 'application/gzip'
        }
      }
    }
  });
  console.log(response);
}

function setToCollectionName(dir) {
  let files = readdirSync(`${dir}/collections`);
  const _dir = `${dir}/collections/${files[0]}`
  files = readdirSync(_dir);
  for (const file of files) {
    const filePath = path.join(_dir, file);
    if (statSync(filePath).isFile() && file.endsWith('.yaml')) {
      const fileContent = readFileSync(filePath, 'utf8');
      const parsedYaml = yaml.load(fileContent);
      const collection = config.collections.find((collection) => collection.database_id === TO_DATABASE_ID);
      parsedYaml.name = collection?.name || parsedYaml.name;
      parsedYaml.slug = collection?.slug || collection?.name?.toLowerCase().split(' ').join('_') || parsedYaml.slug;
      parsedYaml['serdes/meta'][0].label = parsedYaml.slug;
      const newYamlContent = yaml.dump(parsedYaml);
      writeFileSync(filePath, newYamlContent, 'utf8');
      return;
    }
  }
}

function findYamlFiles(dir, fileList = []) {
  const files = readdirSync(dir);
  files.forEach(file => {
    const filePath = path.join(dir, file);
    if (statSync(filePath).isDirectory()) {
      findYamlFiles(filePath, fileList);
    } else if (file.endsWith('.yaml')) {
      fileList.push(filePath);
    }
  });
  return fileList;
}

function deleteArchivedYamlFiles(files) {
  files.forEach(file => {
    const fileContent = readFileSync(file, 'utf8');
    const parsedYaml = yaml.load(fileContent);
    if (parsedYaml.archived === true) {
      unlinkSync(file);
    }
  });
}

const syncCollection = async (fromDatabaseId, toDatabaseId) => {
  // delete tmp directory if it exists
  if (existsSync(path.join(__dirname, 'tmp'))) rmSync(path.join(__dirname, 'tmp'), { recursive: true });
  mkdirSync(path.join(__dirname, 'tmp'));
  const directory = await exportCollection(fromDatabaseId);
  deleteArchivedYamlFiles(findYamlFiles(directory));
  const nanoids = findNanoidsInDirectory(directory);
  nanoids.forEach(nanoid => {
    const entity = config.entities.find((entity) => entity[fromDatabaseId] === nanoid);
    if (!entity) {
      config.entities.push({
        [fromDatabaseId]: nanoid,
        [toDatabaseId]: Nanoid()
      });
    } else {
      entity[toDatabaseId] = entity[toDatabaseId] || Nanoid();
    }
  });
  writeFileSync('./metabase-sync.config.json', JSON.stringify(config, null, 2));
  for (const entity of config.entities) {
    replaceStringInDirectory(directory, entity[fromDatabaseId], entity[toDatabaseId]);
  }
  replaceStringInDirectory(directory, fromDatabaseId, toDatabaseId);
  setToCollectionName(directory);
  await importCollection(directory);
}

syncCollection(FROM_DATABASE_ID, TO_DATABASE_ID);