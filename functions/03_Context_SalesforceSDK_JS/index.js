
import jsforce from "jsforce";
import { request } from "undici";

/**
 * Receives a payload containing account details, and creates the record.
 * It then uses a SOQL query to return the newly created Account.
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
    `Invoking salesforcesdkjs Function with payload ${JSON.stringify(
      event.data || {}
    )}`
  );

  const results = {
    success: 0,
    failures: 0
  }


    // Establish JSForce Connection from Context
    const conn = new jsforce.Connection({
      accessToken: context.org.dataApi.accessToken,
      instanceUrl: context.org.baseUrl,
      version: context.org.apiVersion
    });

    //Direct call to Bulk API v1 
    //GetAllJobs call is not implemented in jsforce
    // Extract dataApi information from context
    const { accessToken, baseUrl, apiVersion } = context.org.dataApi;

      // Setup Bulk API Authorization headers
  const authHeaders = {
    Authorization: `Bearer ${accessToken}`
  };

    // Construct API URL for Bulk API v1
    const apiUrl = `${baseUrl}/services/data/v${apiVersion}`;

    // Query All Jobs
    //jobType=Classic is to filter just Bulk API v1 jobs. Delete this param if you want to retrieve all jobs
    const { statusCode: statusCodeJob, body: bodyJob } = await request(
      `${apiUrl}/jobs/ingest/?jobType=Classic`,
      {
        method: "GET",
        headers: {
          "Content-Type": "application/json",
          ...authHeaders
        },
        body: JSON.stringify({})
      }
    );

    // Get Job Response
  const getAllJobs = await bodyJob.json();

  if (statusCodeJob !== 200) {
    logger.error(JSON.stringify(getAllJobs));
    throw new Error(`Get All Jobs `);
  }
  else{
    //chequear el valor de la variable Done para ver si es necesario hacer más llamadas
    
    
    logger.info('Resultados');
    /*getAllJobs.records.forEach(function(table){
      var jobId = table.id;
      var Operation = table.operation;
      if (Operation!=="query"){
        logger.info("Review Job: " +jobId + " - " + Operation);
      }
      
    })*/

      //Esto igual hay que hacerlo con un reduce
    for(const job of getAllJobs.records){
      if (job.operation!=="query"){
        logger.info("For Review Job: " +job.id + " - " + job.operation);
      } 
    }


    //logger.info(JSON.stringify(getAllJobs));
  }

  // Extract Properties from Payload
  const { name, accountNumber, industry, type, website } = event.data;

  // Validate the payload params
  if (!name) {
    throw new Error(`Please provide account name`);
  }

  // Define a record using the RecordForCreate type and providing the Developer Name
  const account = {
    type: "Account",
    fields: {
      Name: `${name}-${Date.now()}`,
      AccountNumber: accountNumber,
      Industry: industry,
      Description: context.org.dataApi.accessToken,
      Type: type,
      Website: website
    }
  };

  

  // Call Bulk API to get a list of datajobs
  try {
    // Solo como ejemplo
    conn.bulk.query("SELECT Id, Name, NumberOfEmployees FROM Account")
    .on('record', function(rec) { console.log(rec); results.success++;})
    .on('error', function(err) { console.error(err); results.failures++;});
    
  } catch (err) {
    // Catch any DML errors and pass the throw an error with the message
    const errorMessage = `Failed to call Bulk API. Root Cause: ${err.message}`;
    logger.error(errorMessage);
    throw new Error(errorMessage);
  }

  
  try {
    // Insert the record using the SalesforceSDK DataApi and get the new Record Id from the result
    const { id: recordId } = await context.org.dataApi.create(account);

    // Query Accounts using the SalesforceSDK DataApi to verify that our new Account was created.
    const soql = `SELECT Fields(STANDARD) FROM Account WHERE Id = '${recordId}'`;
    const queryResults = await context.org.dataApi.query(soql);
    return queryResults;
  } catch (err) {
    // Catch any DML errors and pass the throw an error with the message
    const errorMessage = `Failed to insert record. Root Cause: ${err.message}`;
    logger.error(errorMessage);
    throw new Error(errorMessage);
  }
  
  
}
