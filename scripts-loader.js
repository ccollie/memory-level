'use strict'
const fs = require('fs')
const path = require('path')
const promisify = require('util').promisify
const { CONNECTION_CLOSED_ERROR_MSG } = require('ioredis/built/utils')

const readFile = promisify(fs.readFile)
const readdir = promisify(fs.readdir)

const scriptMap = new Map()
const initializedClients = new WeakSet()

function splitFilename (filePath) {
  const longName = path.basename(filePath, '.lua')
  const [name, num] = longName.split('-')
  const numberOfKeys = num && parseInt(num, 10)
  return { name, numberOfKeys }
}

async function loadCommand (filename) {
  filename = path.resolve(filename)
  let cmd = scriptMap.get(filename)
  if (!cmd) {
    const lua = (await readFile(filename)).toString()
    const { name, numberOfKeys } = splitFilename(filename)
    cmd = { name, options: { numberOfKeys, lua } }
    scriptMap.set(filename, cmd)
  }
  return cmd
}

function preload (client) {
  if (initializedClients.has(client)) {
    return Promise.resolve()
  }

  function complete (commands) {
    commands.forEach(command => {
      // Only define the command if not already defined
      if (!client[command.name]) {
        client.defineCommand(command.name, command.options)
      }
    })
    initializedClients.add(client)
  }

  const promise = (scriptMap.size === 0) ? loadScripts() : Promise.resolve(Array.from(scriptMap.values()))
  return promise.then(complete)
}

/**
 * Load redis lua scripts.
 * The name of the script must have the following format:
 *
 * cmdName-numKeys.lua
 *
 * cmdName must be in camel case format.
 *
 * For example:
 * moveToFinish-3.lua
 *
 */
async function loadScripts () {
  const dir = path.normalize(path.join(__dirname, 'luascripts'))
  const files = await readdir(dir)

  const luaFiles = files.filter((file) => path.extname(file) === '.lua')

  if (luaFiles.length === 0) {
    /**
    * To prevent unclarified runtime error "updateDelayset is not a function
    * @see https://github.com/OptimalBits/bull/issues/920
    */
    throw new Error('No .lua files found!')
  }

  const commands = []

  for (let i = 0; i < luaFiles.length; i++) {
    const file = path.join(dir, luaFiles[i])
    const command = await loadCommand(file)
    commands.push(command)
  }

  return commands
}

/**
 *  @param {Redis} client
 */
async function connect (client) {
  await preload(client)

  if (client.status === 'ready') {
    return Promise.resolve()
  }

  if (client.status === 'wait') {
    return client.connect()
  }

  if (client.status === 'end') {
    throw new Error(CONNECTION_CLOSED_ERROR_MSG)
  }

  return new Promise((resolve, reject) => {
    let lastError

    const errorHandler = (err) => {
      lastError = err
    }

    const handleReady = () => {
      client.removeListener('end', endHandler)
      client.removeListener('error', errorHandler)
      resolve()
    }

    const endHandler = () => {
      client.removeListener('ready', handleReady)
      client.removeListener('error', errorHandler)
      reject(lastError || new Error(CONNECTION_CLOSED_ERROR_MSG))
    }

    client.once('ready', handleReady)
    client.on('end', endHandler)
    client.once('error', errorHandler)
  })
}

exports.connect = connect
exports.preload = preload
