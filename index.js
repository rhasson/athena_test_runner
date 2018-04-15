const fs = require('fs')
const path = require('path')
const util = require('util')
const AWS = require('aws-sdk')
const asyncInterval = require('asyncinterval')

const setIntervalAsync = util.promisify(setInterval)

const CLI = require('clui')
const clear = CLI.Clear
const clc = require('cli-color')

/* global variables */
const RESULT_SIZE = 1000
const POLL_INTERVAL = 1000
const RESULT_FILENAME = './results.json'
var START = undefined
var END = undefined

var INDEX = undefined

//let creds = new AWS.SharedIniFileCredentials({filename:'/path_to/.aws/credentials', profile: 'name'});
let session = require('./session.json')
let creds = new AWS.Credentials({
    accessKeyId: session.AccessKeyId,
    secretAccessKey: session.SecretAccessKey,
    sessionToken: session.SessionToken
})
AWS.config.credentials = creds;

let athena = new AWS.Athena({region: 'us-east-1'})

/*************************/

/**
 * Utility functions
 */
function bytesToSize(bytes) {
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB']
    if (bytes === 0) return `${bytes} KB`
    const i = parseInt(Math.floor(Math.log(bytes) / Math.log(1024)), 10)
    if (i === 0) return `${bytes} ${sizes[i]})`
    return `${(bytes / (1024 ** i)).toFixed(2)} ${sizes[i]}`
}
  
function toTB(bytes) {
    const tb = (1024 * 1024 * 1024 * 1024)
    return b = bytes > 0 ? (bytes / tb) : 0
}

function toCost(tb) {
    const dollarPerTb = 5.00
    let b = tb * dollarPerTb
    return `$ ${b > 1 ? b.toFixed(2) : b.toFixed(8)}`
}

function millisToMinutesAndSeconds(millis) {
    let minutes = Math.floor(millis / 60000);
    let seconds = ((millis % 60000) / 1000).toFixed(0);
    return minutes + ":" + (seconds < 10 ? '0' : '') + seconds
}

/**
 * getAllFiles
 * @dir - directory name used to help SQL queries.  One query per file
 * Return - array of absolute paths to query files
 */
function getAllFiles(dir) {
    return fs.readdirSync(dir).reduce((files, file) => {
        let name = path.join(dir, file)
        let isDirectory = fs.statSync(name).isDirectory()
        return isDirectory ? [...files, ...getAllFiles(name)] : [...files, name]
    }, [])
}

/**
 * readFiles
 * @filenames - list of absolute paths to query files
 * Return(Promise) - array of objects each containing a path/filename and query string
 */
function readFiles(filenames) {
    f_promises = filenames.map((filename) => {
        return new Promise((resolve, reject) => {
            fs.readFile(filename, 'utf-8', (err, content) => {
                if (err) return reject(err)
                return resolve({"name": filename, "query": content})
            })
        })
    })
    return Promise.all(f_promises)
}

/**
 * executeQueries
 * @files - array of objects containing path/filename and query string
 * Resturn(Promise) - array of updated @files objects with status and query id
 */
function executeQueries(files) {
    params = require('./queryExecutionParams.json')
    tasks = files.map((file) => {
        params.QueryString = file.query
        return new Promise((resolve, reject) => {
            athena.startQueryExecution(params, (err, results) => {
                if (err) {
                    //console.error("startQueryExecution Error: ", err)
                    file.status = "FAILED"
                    file.error = err.message
                } else {
                    file.id = results.QueryExecutionId
                    file.status = "QUEUED"
                }
                return resolve(file)
            })
        })
    })
    return Promise.all(tasks)
}

/**
 * indexTasks - converts array of file objects to a dictionary for easy indexing
 * @tasks - array of file objects
 * Return - dictionary keyed by query ID with the file object as the value
 */
function indexTasks(tasks) {
    idx = {}
    tasks.forEach((task) => {
        idx[task.id] = task
    })
    return idx
}

/**
 * getRunningJobs - filters all jobs and only returns those with running or queued status
 * @tasks - dictionary of all tasks
 * Return - array of ids of running or queued tasks
 */
function getRunningJobs(tasks) {
    ids = []
    _ids = Object.keys(tasks)
    for (let i = 0; i < _ids.length; i++) {
        task = tasks[_ids[i]]
        if (task.status == "QUEUED" || task.status == "RUNNING") 
        ids.push(task.id)
    }
    return ids
}

/**
 * monitor - used to ge the status of running jobs
 * @tasks - all tasks
 * Return(Promise) - object with dictionary updated running jobs and array of failed jobs
 */
function monitor(tasks) {
    return new Promise((resolve, reject) => {
        ids = getRunningJobs(tasks)
        if (ids.length > 0) {
            athena.batchGetQueryExecution({QueryExecutionIds: ids}, (err, results) => {
                if (err) return reject(err)
                results.QueryExecutions.forEach((item) => {
                    t = tasks[item.QueryExecutionId]
                    t.status = item.Status.State
                    if (t.status == "SUCCEEDED" || t.status == "RUNNING") {
                        t.starttime = item.Status.SubmissionDateTime
                        t.endtime = item.Status.CompletionDateTime
                        t.runtime = item.Statistics.EngineExecutionTimeInMillis
                        t.scanned = item.Statistics.DataScannedInBytes
                    }
                    tasks[item.QueryExecutionId] = t
                })
                return resolve({"runs": tasks, "failed": results.UnprocessedQueryExecutionIds})
            })
        } else {
            //END = +new Date()
            return resolve(undefined)
        }
    })
}

/**
 * consolidateJobs - combines dictionary of all jobs with updated and failed jobs returned from athena.batchGetQueryExecution
 * @jobIndex - main dictionary containing all jobs
 * @jobs - object returned from monitor function
 */
function consolidateJobs(jobIndex, jobs) {   
    ids = Object.keys(jobIndex)
    for (let i = 0; i < ids.length; i++) {
        jobIndex[ids[i]] = jobs.runs[ids[i]]
    }

    jobs.failed.forEach((i) => {
        jobIndex[i.QueryExecutionId].status = "FAILED"
        jobIndex[i.QueryExecutionId].error = i.ErrorMessage
    })
    return jobIndex
}

/**
 * draw - draws the status screen showing test progress
 * @r - jobs dictionary
 */
function draw(r) {
    clear()
    let outputBuffer = new CLI.LineBuffer({
        x: 0,
        y: 0,
        width: 'console',
        height: 'console'
    })

    let message = new CLI.Line(outputBuffer)
        .column('Press Control+C to quit.', 100, [clc.green])
        .fill()
        .store()
    
    let blankLine = new CLI.Line(outputBuffer)
        .fill()
        .store()

    let header = new CLI.Line(outputBuffer)
        .column('Name', 40, [clc.cyan])
        .column('Status', 20, [clc.cyan])
        .column('RunTime', 20, [clc.cyan])
        .column('DataScanned', 20, [clc.cyan])
        .fill()
        .store()

    Object.keys(r).forEach((id) => {
        item = r[id]
        let statusColor = [clc.green]
        switch(item.status) {
            case 'FAILED':
                statusColor = [clc.red]
                break
            case 'CANCELLED':
                statusColor = [clc.red]
                break
            case 'RUNNING':
                statusColor = [clc.yellow]
                break
            case 'QUEUED':
                statusColor = [clc.yellow]
                break
        }

        let line = new CLI.Line(outputBuffer)
            .column(`${item.name}`, 40, [clc.white])
            //.column(`${item.status}`, 20, (item.status == 'FAILED' || item.status == 'CANCELLED') ? [clc.red]: [clc.green])
            .column(`${item.status}`, 20, statusColor)
            .column((item.runtime !== undefined) ? `${millisToMinutesAndSeconds(item.runtime)}` : '', 20, [clc.white])
            .column((item.scanned !== undefined) ? `${bytesToSize(item.scanned)}` : '', 20, [clc.white])
            .fill()
            .store()
    })
    outputBuffer.output()
}

/**
 * finishUp - writes out all jobs to json file and exits
 * @jobs - dictionary of all jobs
 */
function finishUp(jobs) {
    fs.writeFileSync(RESULT_FILENAME, JSON.stringify(Object.values(jobs)))
    new CLI.Line().fill().output()
/*
    new CLI.Line()
        .column(`Test run completed in ${END-START} ms`, 80, [clc.green])
        .fill()
        .output()
*/
    new CLI.Line()
        .column(`Job results saved to ${RESULT_FILENAME}`, 80, [clc.green])
        .fill()
        .output()
    new CLI.Line().fill().output()
    process.exit(0)
}

/**
 * run - main program
 * @dirname - directory name where SQL tests are located
 */
async function run(dirname) {
    try {
        START = +new Date()
        filenames = getAllFiles(dirname)
        files = await readFiles(filenames)
        tasks = await executeQueries(files)
        jobIndex = indexTasks(tasks)
        var interval = asyncInterval(async (done) => {
            try {
                jobs = await monitor(jobIndex)
                if (jobs == undefined) finishUp(jobIndex)
                jobIndex = consolidateJobs(jobIndex, jobs)
                INDEX = jobIndex
                draw(jobIndex)
            } catch (e) {
                console.log("Monitor Error: ", e)
                process.exit(1)
            }
        }, 2000, 2000)
    }
    catch (e) {
        console.error("ERROR: ", e)
    }
}

/**
 * Hack to hook CTRL+C on Windows
 */
if (process.platform === "win32") {
    var rl = require("readline").createInterface({
      input: process.stdin,
      output: process.stdout
    })
  
    rl.on("SIGINT", function () {
      process.emit("SIGINT");
    })
}

/**
 * Hook CTRL+C to cancel running queries and save results 
 */
process.on("SIGINT", function () {
    new CLI.Line().fill().output()
    new CLI.Line()
        .column('Cancelling running queries and exiting...', 80, [clc.green])
        .fill()
        .output()
    new CLI.Line().fill().output()
    ids = getRunningJobs(INDEX)
    p = ids.map((id) => {
        return new Promise((resolve, reject) => {
            athena.stopQueryExecution({QueryExecutionId: id}, (err, result) => {
                return resolve()
            })
        })
    })
    Promise.all(p).then(() => { 
        finishUp(INDEX)
        process.exit(0)
    }).catch((e) => { process.exit(0) })
})

/**
 * Get test folder name as command line argument and pass to run function
 */
const input_dir = process.argv[2]
run(input_dir)