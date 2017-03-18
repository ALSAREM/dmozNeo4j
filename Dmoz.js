'use strict';

/**
 * Created by mazen on 01/03/2017.
 */

const bigXml = require('../util/big-xml');
const graphdriver = require('../graphdriver/graphdriver');
const csvWriter = require('csv-write-stream');
const fs = require('fs');
const Promise = require('bluebird');

exports.importDmozCSVinKG = function (inFile, callback) {
    const queries = [
        `
      USING PERIODIC COMMIT 500
      load csv WITH HEADERS from "file:///${inFile}.topics.csv" as row
      With row 
        MERGE (a:DmozTopic{id:row.id})
        ON MATCH SET a+=row
        ON CREATE SET a+=row
    `,
        `
     USING PERIODIC COMMIT 500
      load csv WITH HEADERS from "file:///${inFile}.narrows.csv" as row
      With row 
            MERGE (s:DmozTopic {id:row.fromId})
            ON MATCH SET s+={id:row.fromId,name:row.fromName,fullName:row.fromFullName}
            ON CREATE SET s+={id:row.fromId,name:row.fromName,fullName:row.fromFullName}
            MERGE (o:DmozTopic {id:row.toId})
            ON MATCH SET o+={id:row.toId}
            ON CREATE SET o+={id:row.toId}
            MERGE (s)-[r:DmozSubTopicOf]->(o)
            ON MATCH SET r+={tag:row.relationTag}
            ON CREATE SET r+={tag:row.relationTag}
    `,
        `
     USING PERIODIC COMMIT 500
      load csv WITH HEADERS from "file:///${inFile}.relateds.csv" as row
      With row 
            MERGE (s:DmozTopic {id:row.fromId})
            ON MATCH SET s+={id:row.fromId,name:row.fromName,fullName:row.fromFullName}
            ON CREATE SET s+={id:row.fromId,name:row.fromName,fullName:row.fromFullName}
            MERGE (o:DmozTopic {id:row.toId})
            ON MATCH SET o+={id:row.toId}
            ON CREATE SET o+={id:row.toId}
            MERGE (s)-[r:DmozRelatedTopic]->(o)
    `,
        `
     USING PERIODIC COMMIT 500
      load csv WITH HEADERS from "file:///${inFile}.symbolics.csv" as row
      With row 
            MERGE (s:DmozTopic {id:row.fromId})
            ON MATCH SET s+={id:row.fromId}
            ON CREATE SET s+={id:row.fromId}
            MERGE (o:DmozTopic {id:row.toId})
            ON MATCH SET o+={id:row.toId,name:row.toName}
            ON CREATE SET o+={id:row.toId,name:row.toName}
            MERGE (s)-[r:DmozSymbolicRelated]->(o)
            ON MATCH SET r+={tag:row.relationTag,name:row.relationName}
            ON CREATE SET r+={tag:row.relationTag,name:row.relationName}
    `,
        `
     USING PERIODIC COMMIT 500
      load csv WITH HEADERS from "file:///${inFile}.editors.csv" as row
      With row 
            MERGE (s:DmozTopic {id:row.fromId})
            ON MATCH SET s+={id:row.fromId}
            ON CREATE SET s+={id:row.fromId}
            MERGE (o:DmozEditor {id:row.toId})
            ON MATCH SET o+={id:row.toId,name:row.toName}
            ON CREATE SET o+={id:row.toId,name:row.toName}
            MERGE (s)-[r:DmozHasEditor]->(o)
    `,
        `
      USING PERIODIC COMMIT 500
      load csv WITH HEADERS from "file:///${inFile}.langs.csv" as row
      With row
            MERGE (s:DmozTopic {id:row.fromId})
            ON MATCH SET s+={id:row.fromId}
            ON CREATE SET s+={id:row.fromId}
            MERGE (o:DmozTopic {id:row.toId})
            ON MATCH SET o+={id:row.toId,name:row.toName}
            ON CREATE SET o+={id:row.toId,name:row.toName}
            MERGE (l:DmozLang {id:row.to2Id})
            ON MATCH SET l+={id:row.to2Id,name:row.to2Name}
            ON CREATE SET l+={id:row.to2Id,name:row.to2Name}
            MERGE (s)-[r:DmozSameAs]->(o)-[rr:DmozHasLang]->(l)
            ON MATCH SET r+={lang:row.relationLang}, rr+={lang:row.relation2Lang}
            ON CREATE SET r+={lang:row.relationLang}, rr+={lang:row.relation2Lang}
    `,
    ];

    // topics
    winston.log('info', 'Start importing topics');
    winston.log('info', queries[0]);
    graphdriver.runQuery(queries[0], () => {
        winston.log('info', `done topics csv importing from ${inFile}`);
        // narrwos
        winston.log('info', queries[1]);
        graphdriver.runQuery(queries[1], () => {
            winston.log('info', `done narrows csv importing from ${inFile}`);
            // relateds
            winston.log('info', queries[2]);
            graphdriver.runQuery(queries[2], () => {
                winston.log('info', `done relateds csv importing from ${inFile}`);
                // symbolics
                winston.log('info', queries[3]);
                graphdriver.runQuery(queries[3], () => {
                    winston.log('info', `done Symbolics csv importing from ${inFile}`);
                    // editors
                    winston.log('info', queries[4]);
                    graphdriver.runQuery(queries[4], () => {
                        winston.log('info', `done editors csv importing from ${inFile}`);
                        // langs
                        winston.log('info', queries[5]);
                        graphdriver.runQuery(queries[5], () => {
                            winston.log('info', `done langs csv importing from ${inFile}`);
                            callback();
                        }, (err) => {
                            winston.log('debug', `Error langs csv importing from ${inFile}`);
                            winston.trace(err);
                            callback();
                        });
                    }, (err) => {
                        winston.log('debug', `Error editors csv importing from ${inFile}`);
                        winston.trace(err);
                        callback();
                    });
                }, (err) => {
                    winston.log('debug', `Error Symbolics csv importing from ${inFile}`);
                    winston.trace(err);
                    callback();
                });
            }, (err) => {
                winston.log('debug', `Error relateds csv importing from ${inFile}`);
                winston.trace(err);
                callback();
            });
        }, (err) => {
            winston.log('debug', `Error narrows csv importing from ${inFile}`);
            winston.trace(err);
            callback();
        });
    }, (err) => {
        winston.log('debug', `Error topics csv importing from ${inFile}`);
        winston.trace(err);
        callback();
    });
};

exports.contentPages2CSV = function (inFile, outFile, callback) {
  const reader = bigXml.createReader(inFile, /^(ExternalPage|RDF)$/, { gzip: false });
  let counter = 0;
  let fileCounter = 34;
  let counterForFile = 0;
  const maxPerFile = 100000;
  let writer = csvWriter();
  writer.pipe(fs.createWriteStream(`${outFile}_${fileCounter}.csv`));
  reader.on('record', (record) => {
    counter += 1;
    if (counter > 3247275) {
      winston.log('info', `New Record arrive (${record.attrs.about}) with counter ${counter}`);

      counterForFile += 1;
      if (counterForFile > maxPerFile) {
        fileCounter += 1;
        counterForFile = 0;
        writer.end();
        winston.log('info', `Start new File: ${outFile}_${fileCounter}.csv`);
        writer = csvWriter();
        writer.pipe(fs.createWriteStream(`${outFile}_${fileCounter}.csv`));
      }
      let description = record.children.filter(child => child.tag === 'd:Description');
      if (description[0] && description[0].text) {
        description = description[0].text.replace(/"/g, ' ').replace(/'/g, ' ');
      } else description = '';
      let title = record.children.filter(child => child.tag === 'd:Title');
      if (title[0] && title[0].text) {
        title = title[0].text;
      } else title = '';
      let priority = record.children.filter(child => child.tag === 'priority');
      if (priority[0] && priority[0].text) {
        priority = priority[0].text;
      } else priority = -1;
      let topic = record.children.filter(child => child.tag === 'topic');
      if (topic[0] && topic[0].text) {
        topic = topic[0].text;
      } else topic = 'thing';

      let topicid = topic.replace(/\//g, '_').replace(/'/g, '_').toLowerCase();
      if (topicid === '') {
        topicid = 'thing';
      }

      const pageUrl = record.attrs.about;

      const fullText = topic.replace(/\//g, ' ').replace(/'/g, ' ').replace(/_/g, ' ').toLowerCase()
        + description.toLowerCase().replace(/_/g, ' ')
        + title.toLowerCase().replace(/_/g, ' ');
      const stemd = nlp.stem(fullText, 'en');
      const tfidf = nlp.createTFIDFClass([fullText]);
      let topTerms = '';
      nlp.getTermsWithImportance(0 /* document index */, tfidf).forEach((item) => {
        if (item.tfidf > 1) {
          topTerms += `${item.term}, `;
        }
      });
      let topStems = '';
      nlp.getTFWithOptions(fullText, { score: true, stem: true, ngrams: [1, 2, 3], min: 2 }).forEach((item) => {
        topStems += `${item.term}, `;
      });

      const page = {
        id: pageUrl,
        url: pageUrl,
        title,
        description,
        priority,
        topic,
        topicid,
        stemd,
        topTerms,
        topStems,
      };
      writer.write(page);
    }
  });

  reader.on('end', () => {
    writer.end();
    if (callback) callback();
  });
};

exports.importDmozContentCSVinKG = function (inFile, callback) {
  const q = `
  USING PERIODIC COMMIT 500
  load csv WITH HEADERS from "file:///${inFile}" as row
  With row
        MERGE (s:DmozPage {id:row.id})
        ON MATCH SET s+=row
        ON CREATE SET s+=row
        MERGE (o:DmozTopic {id:row.topicid})
        ON MATCH SET o+={id:row.topicid,fullName:row.topic}
        ON CREATE SET o+={id:row.topicid,fullName:row.topic}
        MERGE (s)-[r:DmozHasTopic]->(o)
`;

  winston.log('info', q);
  graphdriver.runQuery(q, () => {
    winston.log('info', `done content (pages) csv importing from ${inFile}`);
    callback();
  }, (err) => {
    winston.log('debug', `Error content (pages) csv importing from ${inFile}`);
    winston.trace(err);
    callback();
  });
};

exports.run = function (execParams) {
  winston.log('info', execParams);
  let inFile = '';
  let outFile = '';
  let from = 0;
  let to = 0;
  let array = [];

  switch (execParams.dmoz) {
    case 'importStructure':// structure part

      profiler.begin('ALL:importStructure');
      inFile = execParams.inFile;// 'csv/structure/structure.part4'
      exports.importDmozCSVinKG(inFile, () => {
        profiler.end('ALL:importStructure');
        profiler.show();
      });
      break;
    case 'importAllStructure':// structure one by one
      profiler.begin('ALL:importAllStructure');
      inFile = execParams.inFile;// 'csv/structure/structure'
      from = parseInt(execParams.from, 10);// 0
      to = parseInt(execParams.to, 10);// 9
      array = [];
      for (let i = from; i <= to; i += 1) {
        array.push(i);
      }
      winston.log('info', `Start importing structure ${inFile}.part${from}_${to}`);
      Promise.mapSeries(array, i => new Promise((resolve) => {
        winston.log('info', `Start importing ${inFile}.part${i}`);
        profiler.begin(`ALL:importContent: ${inFile}.part${i}`);
        exports.importDmozCSVinKG(`${inFile}.part${i}`, () => {
          resolve();
          profiler.end(`ALL:importContent: ${inFile}.part${i}`);
          profiler.show();
        });
      })).then(() => {
        profiler.end('ALL:importAllStructure');
        profiler.show();
      });
      break;
    case 'importContent':// content one by one

      profiler.begin('ALL:importContent');
      inFile = execParams.inFile;// 'csv/content/content.csv'
      exports.importDmozContentCSVinKG(inFile, () => {
        profiler.end('ALL:importContent');
        profiler.show();
      });
      break;
    case 'importAllContent':// content one by one
      profiler.begin('ALL:importAllContent');
      from = parseInt(execParams.from, 10);// 0
      to = parseInt(execParams.to, 10);// 37
      inFile = execParams.inFile;// 'csv/content/content'
      array = [];
      for (let i = from; i <= to; i += 1) {
        array.push(i);
      }
      winston.log('info', `Start importing ${inFile}_${from}_${to}.csv`);
      Promise.mapSeries(array, i => new Promise((resolve) => {
        winston.log('info', `Start importing ${inFile}_${i}.csv`);
        profiler.begin(`ALL:importContent: ${inFile}_${i}.csv`);
        exports.importDmozContentCSVinKG(`${inFile}_${i}.csv`, () => {
          resolve();
          profiler.end(`ALL:importContent: ${inFile}_${i}.csv`);
          profiler.show();
        });
      })).then(() => {
        profiler.end('ALL:importAllContent');
        profiler.show();
      });
      break;
  }
};

