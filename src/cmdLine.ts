#! /usr/bin/env node

import * as yargs from 'yargs'
import { getClusterState, setClusterState, ClusterState } from './cloner'
import { createAPI, schema } from 'dremio-node-api'
import { promisify } from 'util'
import * as fs from 'fs'
import { createCipheriv, pseudoRandomBytes as _pseudoRandomBytes, createDecipheriv } from 'crypto'

const readFile = promisify(fs.readFile)
const writeFile = promisify(fs.writeFile)
const pseudoRandomBytes = promisify(_pseudoRandomBytes)

function zeroFillString(str: string, l: number) {
  for (let i = 0; i < l; i++) {
    str += '0'
  }
  return str
}

async function encryptText(key: string, text: string) {
  if (key.length < 16) {
    key = zeroFillString(key, 16 - key.length)
  } else if (key.length < 24) {
    key = zeroFillString(key, 24 - key.length)
  } else if (key.length < 32) {
    key = zeroFillString(key, 32 - key.length)
  } else if (key.length > 32) {
    key = key.substring(0, 32)
  }

  const iv = await pseudoRandomBytes(16)

  const cipher = createCipheriv(key.length == 16 ? 'AES_128' : key.length == 24 ? 'AES_192' : 'AES_256', key, iv)

  let result = iv.toString() + cipher.update(text, 'utf8', 'hex')
  result += cipher.final('hex')

  return result
}

function decryptText(key: string, text: string) {
  if (key.length < 16) {
    key = zeroFillString(key, 16 - key.length)
  } else if (key.length < 24) {
    key = zeroFillString(key, 24 - key.length)
  } else if (key.length < 32) {
    key = zeroFillString(key, 32 - key.length)
  } else if (key.length > 32) {
    key = key.substring(0, 32)
  }

  const iv = text.substring(0, 16)
  var decipher = createDecipheriv(key.length == 16 ? 'AES_128' : key.length == 24 ? 'AES_192' : 'AES_256', key, iv)

  let result = decipher.update(text.substring(16), 'hex', 'utf8')
  result += decipher.final('utf8')

  return result
}

const argv = yargs
  .command('encrypt <key> <inFileName> <outFileName>', 'Encrypt a file, especially a credentials file',
    () => yargs
      .positional('key', {
        describe: 'The key used to encrypt the file. Should be either 16/24/32 characters long.',
        type: 'string'
      })
      .positional('inFileName', {
        describe: 'The file to encrypt.',
        type: 'string'
      })
      .positional('outFileName', {
        describe: 'The file in which to save the encrypted contents.',
        type: 'string'
      }),
    async (args) => {
      await writeFile(args['outFileName'], await encryptText(args['key'], await readFile(args['inFileName'], 'utf8')))
    })
  .command('save', 'Save the state of an existing cluster',
    () => yargs
      .option({
        host: {
          describe: 'The master hostname of the cluster',
          type: 'string',
          required: true
        },
        port: {
          describe: 'The master port of the cluster',
          type: 'number',
          default: 9047
        },
        ssl: {
          describe: 'Whether the master is an https connection',
          type: 'boolean',
          default: false
        },
        user: {
          describe: 'The username of the admin to connect to the cluster',
          type: 'string',
          required: true
        },
        password: {
          describe: 'The password of the admin to connect to the cluster. Not required if a user credential file is supplied',
          type: 'string'
        },
        userCredFile: {
          describe: 'The file containing user credentials for this cluster',
          type: 'string'
        },
        userCredFileKey: {
          describe: 'The key with which to decrypt the user credentials file',
          type: 'string'
        },
        stateFile: {
          describe: 'The name of the file to save the state to',
          type: 'string',
          default: './dremio_cluster_state.json'
        }
      }),
    async (args) => {
      const clusterState = await createAPI({
        host: args['host'],
        port: args['port'],
        ssl: args['ssl']
      }, {
          userName: args['user'],
          password: args['password']
        })

      await writeFile(args['stateFile'], JSON.stringify(await getClusterState(clusterState)), 'utf8')
    })
  .command('load', 'Load the state of a cluster into a new cluster',
    (args) => yargs
      .option({
        host: {
          describe: 'The master hostname of the cluster',
          type: 'string',
          required: true
        },
        port: {
          describe: 'The master port of the cluster',
          type: 'number',
          default: 9047
        },
        ssl: {
          describe: 'Whether the master is an https connection',
          type: 'boolean',
          default: false
        },
        user: {
          describe: 'The username of the admin to connect to the cluster',
          type: 'string',
          required: true
        },
        password: {
          describe: 'The password of the admin to connect to the cluster. Not required if a user credential file is supplied',
          type: 'string'
        },
        userCredFile: {
          describe: 'The file containing user credentials for this cluster',
          type: 'string'
        },
        userCredFileKey: {
          describe: 'The key with which to decrypt the user credentials file',
          type: 'string'
        },
        sourceCredFile: {
          describe: 'The file containing source credentials for this cluster',
          type: 'string',
          required: true
        },
        sourceCredFileKey: {
          describe: 'The key with which to decrypt the source credentials file',
          type: 'string'
        },
        stateFile: {
          describe: 'The name of the cluster state file to load from',
          type: 'string',
          default: './dremio_cluster_state.json'
        },
        createFirstUser: {
          describe: 'If true, will create the admin user as the first user for the cluster. If false, this user must already exist in the cluster',
          type: 'boolean',
          default: 'false'
        },
        failOnError: {
          describe: 'If true will cause the process to abort upon an error. If false, will try to continue as far as it can.',
          type: 'boolean',
          default: 'false'
        }
      }),
    async (args) => {
      const clusterState = JSON.parse(await readFile(args['stateFile'], 'utf8')) as ClusterState

      let sourceCredFile = await readFile(args['sourceCredFile'], 'utf8')
      if (args['sourceCredFileKey']) {
        sourceCredFile = decryptText(args['sourceCredFileKey'], sourceCredFile)
      }
      const sourceCreds = JSON.parse(sourceCredFile)

      let userCreds: schema.LoginBodyArgs[]
      if (args['userCredFile']) {
        let userCredFile = await readFile(args['userCredFile'], 'utf8')
        if (args['userCredFileKey']) {
          userCredFile = decryptText(args['userCredFileKey'], userCredFile)
        }
        userCreds = JSON.parse(userCredFile)
      } else {
        userCreds = [{
          userName: args['user'],
          password: args['password']
        }]
      }

      await setClusterState(
        clusterState,
        sourceCreds,
        args['failOnError'],
        process.stdout,
        process.stderr,
        {
          host: args['host'],
          port: args['port'],
          ssl: args['ssl']
        },
        userCreds,
        args['user'],
        args['createFirstUser']
      )
    })
  .help('help').argv

const taskName = argv._[0]

if (!taskName) {
  yargs.showHelp()
}
