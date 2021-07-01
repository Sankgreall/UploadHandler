// Require Azure Identity packages
const { DefaultAzureCredential, ManagedIdentityCredential } = require('@azure/identity');
const { SecretClient } = require("@azure/keyvault-secrets");
const { ShareServiceClient, ShareFileClient } = require("@azure/storage-file-share")
const { QueueServiceClient } = require("@azure/storage-queue");

// Get managed identity
credential = getCredentials()

/* Storage account setup */

// Register blob storage account
const externalDataUrl = process.env["externalDataUrl"]
const externalDataSecretName = process.env["externalDataSecretName"]

// Register file storage account
const internalDataUrl = process.env["internalDataUrl"]
const internalDataSecretName = process.env["internalDataSecretName"]

// Get vault instance
const vaultName = process.env["vaultName"]
var client = authenticateVault(vaultName, credential);

// Generate SAS tokens
var internalDataSAS_Promise = generateSAS(client, internalDataSecretName)
var externalDataSAS_Promise = generateSAS(client, externalDataSecretName)

// Get handle to internal data share
var internalDataClient_Promise = getStorageAccount(client, internalDataSecretName, internalDataUrl)

/* Queue account setup */
const processingQueueUrl = process.env["processingQueueUrl"]
const processingQueueSecretName = process.env["processingQueueSecretName"]

var queue_Promise = generateSAS(client, processingQueueSecretName).then((res) => {
    var queueClient = new QueueServiceClient(processingQueueUrl + res.value)
    return queue = queueClient.getQueueClient("processing-tasks")
})

// @returns Credential object
function getCredentials()
{
    // ManagedIdentityCredential created by "identity assign" command
   return new DefaultAzureCredential();
}

// @returns Vault client object
function authenticateVault(vaultName, credential)
{
    let vaultUrl = `https://${vaultName}.vault.azure.net`;    
    return new SecretClient(vaultUrl, credential);
}

async function toProcess(message, urlSplit, queue)
{
    // Get the file path of the item (5th+ elements)
    let filePathArray = urlSplit.slice(4, urlSplit.length)

    // Get file extention
    ext = filePathArray[filePathArray.length - 1].split(".")[1]
    if(!ext)
    {
        // File has no extension, skip it
        return false
    }
    
    // If file is inside Surge directory and has .zip ext, move to queue
    if(filePathArray[0].toLowerCase() == "surge" && ext == "zip")
    {
        queue.sendMessage(JSON.stringify(message)).catch((error) => {console.log(error)})
    }
} 

async function createDirectoryStructure(filePath, internalDataClient, containerName)
{
    // Map folder structure
    folderStructure = filePath.split("/")
            
    // Create a handle on the file share
    var shareClient = internalDataClient.getShareClient(containerName)

    // For each folder, create it
    for (let i = 0; i < (folderStructure.length - 1); i++) // -1 due to file at the end
    {
        // Get the incremental directory path
        let directory = folderStructure.slice(0, (i + 1)).join("/")

        // Open a handle on the directory to create
        let currentDirectoryClient = shareClient.getDirectoryClient(directory)
        
        // Attempt to create the directory
        try
        {
            await currentDirectoryClient.create()
        }
        catch(error)
        {
            if (error == "ResourceAlreadyExists")
            {
                // Jump to the end to see if someone beat us to it
                let directory = folderStructure.slice(0, folderStructure.length - 2).join("/")
                let endDirectoryClient = shareClient.getDirectoryClient(directory)
                if (await endDirectoryClient.exists())
                {
                    // break the loop, we're done!
                    return true
                }
            }
        }
    }

    return true
}

async function copy(url, urlSplit, dstAccountUrl, dstSAS, srcSAS, internalDataClient)
{
    // Get the name of the blob container (will always be 4th element)
    let containerName = urlSplit[3]

    // Get the file path of the item (5th+ elements)
    let filePath = urlSplit.slice(4, urlSplit.length).join("/")

    // Construct the destination object from its URL
    let dstFileUrl = dstAccountUrl + "/" + containerName + "/" + filePath + dstSAS.value
    let dstFileObject = new ShareFileClient(dstFileUrl)
    
    // Construct the source URL
    let srcFileUrl = url + srcSAS.value

    dstFileObject.startCopyFromURL(srcFileUrl).then(() => {
        return true
    }).catch((error) => {

        if (error.code == "ShareNotFound")
        {
            // We need to create the destination share
            // Get handle on storage account
            internalDataClient.createShare(containerName).then((res) => {copy(url, urlSplit, dstAccountUrl, dstSAS, srcSAS, internalDataClient)}).catch((error) => {return false})
        }
        else if (error.code == "ParentNotFound")
        {
            // Create directory structure
            createDirectoryStructure(filePath, internalDataClient, containerName).then((res) => {copy(url, urlSplit, dstAccountUrl, dstSAS, srcSAS, internalDataClient)}).catch((error) => {return false})
        }
        else
        {
            console.log("Error: " + error.code)
            return false
        }
    })
}

async function generateSAS(client, secretName)
{
    return client.getSecret(secretName)
}

async function getStorageAccount(client, secretName, url)
{
    SAS = await generateSAS(client, secretName)
    accountClient = new ShareServiceClient(url + SAS.value)
    return accountClient
}


module.exports = async function (context, message) {

    /* Basic preprocessing */
    // Get event type
    let eventType = message.eventType

    // Split the URL into component parts
    let urlSplit = message.data.url.split("/")

    if (eventType == "Microsoft.Storage.BlobCreated")
    {
        promises = Promise.all([internalDataSAS_Promise, externalDataSAS_Promise, internalDataClient_Promise, queue_Promise])
        // Wait untill all promises are resolved
        promises.then((results) => {

            // Capture the values
            var internalDataSAS = results[0]
            var externalDataSAS = results [1]
            var internalDataClient = results[2]
            var queue = results[3]
           
            // Check SAS hasn't expired
            dateCheck = new Date()
            dateCheck.setHours(dateCheck.getHours() + 4)
            if(internalDataSAS.properties.expiresOn < dateCheck) // Expires soon
            {
                // Generate SAS tokens
                let internalDataSAS_Promise = generateSAS(client, internalDataSecretName)
                let externalDataSAS_Promise = generateSAS(client, externalDataSecretName)
                let queue_Promise = generateSAS(client, processingQueueSecretName).then((res) => {
                    var queueClient = new QueueServiceClient(processingQueueUrl + res.value)
                    return queue = queueClient.getQueueClient("processing-tasks")
                })

                // Get handle to internal data share
                let internalDataClient_Promise = getStorageAccount(client, internalDataSecretName, internalDataUrl)

                let promises = Promise.all([internalDataSAS_Promise, externalDataSAS_Promise, internalDataClient_Promise, queue_Promise])
                promises.then((results) => {

                    // Capture the values
                    var internalDataSAS = results[0]
                    var externalDataSAS = results [1]
                    var internalDataClient = results[2]
                    var queue = results[3]

                    // See if we need to process the file
                    toProcess(message, urlSplit, queue)

                    // Copy the file to internal share
                    copy(
                        message.data.url,
                        urlSplit,
                        internalDataUrl,
                        internalDataSAS,
                        externalDataSAS,
                        internalDataClient
                    )

                }).catch((error) => {console.log(error)})
            }

            else
            {
                    // See if we need to process the file
                    toProcess(message, urlSplit, queue)

                    // Copy the file to internal share
                    copy(
                        message.data.url,
                        urlSplit,
                        internalDataUrl,
                        internalDataSAS,
                        externalDataSAS,
                        internalDataClient
                    )
            }       
        }).catch((error) => {console.log(error)})

    }
};