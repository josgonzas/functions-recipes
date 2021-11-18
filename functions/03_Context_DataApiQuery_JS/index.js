import axios from "axios";
import csvToJson from "csvtojson";
import convert from "xml-js";
import admZip from "adm-zip";
import fs from "fs-extra";
import request from "request";
import converter from "json-2-csv";
/**
 * Returns accounts and its contacts by keyword.
 *
 * The exported method is the entry point for your code when the function is invoked.
 *
 * Following parameters are pre-configured and provided to your function on execution:
 * @param event: represents the data associated with the occurrence of an event, and
 *                 supporting metadata about the source of that occurrence.
 * @param context: represents the connection to Functions and your Salesforce org.
 * @param logger: logging handler used to capture application logs and trace specifically
 *                 to a given execution of a function.
 */
export default async function (event, context, logger) {
  logger.info(
    `Invoking datapiqueryjsssss Function with payload ${JSON.stringify(
      event.data || {}
    )}`
  );

  const payload = event.data;

  const jobId = payload.job;
  const baseUrl = payload.baseUrl;

  const accessToken = payload.accessToken;
  const CSV = "CSV";
  const ZIP_CSV = "ZIP_CSV";

  const getJobDetails = async (job) => {
    try {
      const getBatchResult = `${baseUrl}/services/async/53.0/job/${job}`;
      const headerBatchResult = {
        headers: {
          "Content-Type": "application/json",
          "X-SFDC-Session": `${accessToken}`,
          responseType: 'blob'
        }
      };
        const resp = await axios.get(getBatchResult,headerBatchResult);
        //console.log(resp.data);
        return resp.data;
    } catch (err) {
        // Handle Error Here
        console.error(err);
        return null;
    }
  };

  const getBatchResult = async (job,batch) => {
    try {
      const getBatchResult = `${baseUrl}/services/async/53.0/job/${job}/batch/${batch}/result`;
      const headerBatchResult = {
        headers: {
          "Content-Type": "application/json",
          "X-SFDC-Session": `${accessToken}`,
          responseType: 'blob'
        }
      };
        const resp = await axios.get(getBatchResult,headerBatchResult);
        //console.log(resp.data);
        return resp.data;
    } catch (err) {
        // Handle Error Here
        console.error(err);
        return null;
    }
  };

  const getBatchRequest = async (job,batch) => {
    try {
      const getBatchResult = `${baseUrl}/services/async/53.0/job/${job}/batch/${batch}/request`;
      const headerBatchResult = {
        headers: {
          "Content-Type": "application/json",
          "X-SFDC-Session": `${accessToken}`,
          responseType: 'blob'
        }
      };
        const resp = await axios.get(getBatchResult,headerBatchResult);
        //console.log(resp.data);
        return resp.data;
    } catch (err) {
        // Handle Error Here
        console.error(err);
        return null;
    }
  };

  const getBatchRequestZipFile = async (job,batch) => {
    return new Promise((resolve, reject) => {
      var options = {
        url: `${baseUrl}/services/async/53.0/job/${job}/batch/${batch}/request`,
        headers: {
          "X-SFDC-Session": `${accessToken}`
          }
        }
    let req = request(options).pipe(fs.createWriteStream('request.zip'));
    req.on('finish',function(){
      const zip = new admZip('request.zip');
        const entries = zip.getEntries();
        //console.log("Termino");
        //console.log(entries);
        for(let entry of entries) {
            const buffer = entry.getData();
            //console.log("File: " + entry.entryName + ", length (bytes): " + buffer.length + ", contents: " + buffer.toString("utf-8"));
          resolve (buffer.toString("utf-8"))
      }
    })
    req.on('error', (err) => {
      reject(err)
    })
    req.on('timeout', () => {
      request.destroy()
      reject(new Error('timed out'))
    })
  })
}

  const getAllBatches = async (job) => {
    try {
      const getBatchResult = `${baseUrl}/services/async/53.0/job/${job}/batch`;
      const headerBatchResult = {
        headers: {
          "Content-Type": "application/json",
          "X-SFDC-Session": `${accessToken}`,
          responseType: 'blob'
        }
      };
        const resp = await axios.get(getBatchResult,headerBatchResult);
        //console.log(resp.data);
        return resp.data;
    } catch (err) {
        // Handle Error Here
        console.error(err);
        return null;
    }
  };

  let json2csvCallback = function (err, csv) {
    if (err) throw err;
    console.log(csv);
 };


try {
  console.log("********************************");
  console.log("JOB DETAIL");
  console.log("********************************");
  let jobDetail = await getJobDetails(jobId);
  console.log("********************************");
  console.log("JOB BATCHES");
  console.log("********************************");
  let allBatches = await getAllBatches(jobDetail.id);
  let stringAllBatches= convert.xml2json(allBatches, {compact: true, spaces: 4});
  console.log(stringAllBatches);
  let jsonAllBatches = JSON.parse(stringAllBatches);
  console.log(jsonAllBatches);
  let jsonArray = jsonAllBatches.batchInfoList.batchInfo;
  console.log(jsonArray[0]);
  console.log(`batch info length: ${jsonArray.length}`);
  let arrayRequestsFailed = [];
  for (let i=0; i < jsonArray.length; i++){
    console.log("********************************");
    console.log("BATCH ");
    console.log("********************************");
    let batchIdentifier = jsonArray[i].id._text;
    console.log(batchIdentifier);
    let requestFile;
    let jsonRequest;
    if (jobDetail.contentType == ZIP_CSV){
      console.log("********************************");
      console.log("BATCH ZIP FILE ");
      console.log("********************************");
      requestFile = await getBatchRequestZipFile(jobId,batchIdentifier);
      //console.log(requestFile);
      jsonRequest= await csvToJson().fromString(requestFile);
      console.log(jsonRequest[3]);
    }
    if (jobDetail.contentType == CSV){
      console.log("********************************");
      console.log("BATCH CSV FILE ");
      console.log("********************************");
      requestFile = await getBatchRequest(jobId,batchIdentifier);
      //console.log(requestFile);
      jsonRequest= await csvToJson().fromString(requestFile);
      console.log(jsonRequest[3]);
    }
    console.log("********************************");
    console.log("BATCH RESULT");
    console.log("********************************");
    let resultReponse = await getBatchResult(jobId,batchIdentifier);
    let jsonResult= await csvToJson().fromString(resultReponse);
    console.log(jsonResult[3]);
    console.log(`request info length: ${jsonRequest.length}`);
    console.log(`result info length: ${jsonResult.length}`);
    for (let j=0; j < jsonResult.length; j++){
      if (jsonResult[j].Success=='false'){
        arrayRequestsFailed.push(jsonRequest[j]);
      }
    }
  }
  console.log("********************************");
  console.log("FALLIDOS");
  console.log("********************************");
  console.log(arrayRequestsFailed.length);
  converter.json2csv(arrayRequestsFailed, json2csvCallback);
} catch (err) {
  console.log(err);
}

}
