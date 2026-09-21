import { createRequire } from 'node:module'

const require = createRequire(import.meta.url)
const assert = require('node:assert/strict')
const crypto = require('node:crypto')
const fs = require('node:fs')
const http = require('node:http')
const os = require('node:os')
const path = require('node:path')
const { once } = require('node:events')
const WebSocket = require('ws')
const plugin = (await import('../lib/index.mjs')).default
const { sweepTempUploadDirectory } = await import('../lib/index.mjs')

const SESSION_ID = 'upload-session'
const workspace = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-mobile-upload-'))
const outside = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-mobile-upload-outside-'))
fs.mkdirSync(path.join(workspace, 'nested'))
fs.writeFileSync(path.join(workspace, 'nested', 'existing.txt'), Buffer.from('original content\n'))
fs.writeFileSync(path.join(outside, 'secret.bin'), Buffer.from('outside the session workspace\n'))
fs.symlinkSync(outside, path.join(workspace, 'escape-dir'))

function waitFor(predicate, timeout = 3000) {
  return new Promise((resolve) => {
    const started = Date.now()
    const timer = setInterval(() => {
      if (predicate()) { clearInterval(timer); resolve(true) } else if (Date.now() - started > timeout) { clearInterval(timer); resolve(false) }
    }, 10)
  })
}

// Every stub is the same shape the other gateway tests use: one Host Remote
// Gateway whose session list reports the upload workspace as the session cwd.
async function startGateway(config) {
  const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-mobile-upload-gateway-'))
  let managementRoute
  let upgradeRoute
  let disposePlugin = null
  const server = http.createServer((req, res) => {
    if (managementRoute && (req.url === managementRoute.path || req.url.startsWith(`${managementRoute.path}/`))) {
      managementRoute.handler(req, res)
      return
    }
    res.writeHead(404).end()
  })
  const webServer = {
    port: 0,
    register(route) { managementRoute = route; return () => { managementRoute = undefined } },
    registerUpgrade(route) { upgradeRoute = route; return () => { upgradeRoute = undefined } },
  }
  server.on('upgrade', (req, socket, head) => {
    if (upgradeRoute) upgradeRoute.handler(req, socket, head)
    else socket.destroy()
  })
  const ctx = {
    webServer,
    typertGateway: {
      async invoke(request) {
        if (request.namespace === 'session' && request.method === 'list') {
          return { items: [{ sessionId: SESSION_ID, cwd: workspace, updatedAt: 1, running: false, blank: false }] }
        }
        throw new Error(`unexpected Remote invocation ${request.namespace}/${request.method}`)
      },
      async stream(request) {
        return (async function* () {
          if (request.namespace === 'workspace' && request.method === 'follow') {
            yield { type: 'baseline', value: { items: [], archivedSessionIds: [] } }
          } else if (request.namespace === 'session' && request.method === 'control') {
            yield { type: 'baseline', value: { queues: {}, jobs: {}, projections: {} } }
          } else {
            throw new Error(`unexpected Remote stream ${request.namespace}/${request.method}`)
          }
          while (!request.signal.aborted) await new Promise((resolve) => setTimeout(resolve, 5))
        })()
      },
    },
    agentDefaultModel: {},
    on() { return () => {} },
    effect(factory) { disposePlugin = factory() },
  }
  plugin.apply(ctx, {
    requireAuth: false,
    gatewayEnabled: true,
    gatewayWaitTimeoutMs: 60_000,
    deviceFile: path.join(temp, 'devices.json'),
    publicUrlFile: path.join(temp, 'missing-public-url'),
    // Each stub gateway gets its own temp upload directory so a sweep never
    // touches the deployment default.
    fileUploadTempDirectory: fs.mkdtempSync(path.join(temp, 'uploads-')),
    ...config,
  })
  server.listen(0, '127.0.0.1')
  await once(server, 'listening')
  webServer.port = server.address().port
  return {
    url: `ws://127.0.0.1:${webServer.port}/ws/mobile`,
    async close() {
      if (disposePlugin) disposePlugin()
      server.close()
      await once(server, 'close')
      fs.rmSync(temp, { recursive: true, force: true })
    },
  }
}

async function connect(url) {
  const ws = new WebSocket(url)
  const frames = []
  ws.on('message', (data) => frames.push(JSON.parse(data.toString())))
  await once(ws, 'open')
  const client = { ws, frames }
  assert.equal(await waitFor(() => frames.some((frame) => frame.kind === 'hello')), true, 'hello must arrive')
  client.hello = frames.find((frame) => frame.kind === 'hello')
  return client
}

async function request(client, frame, match = () => true, timeout = 3000) {
  const before = client.frames.length
  client.ws.send(JSON.stringify(frame))
  const ready = await waitFor(() => client.frames.slice(before).some(match), timeout)
  if (!ready) {
    throw new Error(`${frame.type} produced no matching reply; got ${JSON.stringify(client.frames.slice(before)).slice(0, 400)}`)
  }
  return client.frames.slice(before).find(match)
}

const isTransferReply = (frame) => frame.kind === 'error' || String(frame.kind).startsWith('file-upload-')

const openUpload = (client, payload) => request(client, { type: 'file-upload-open', sessionId: SESSION_ID, ...payload },
  (frame) => frame.kind === 'file-upload-opened' || frame.kind === 'error')

const writeUpload = (client, transferId, offset, buffer, extra = {}) => request(client,
  { type: 'file-upload-write', transferId, offset, data: buffer.toString('base64'), ...extra }, isTransferReply)

const cancelUpload = (client, transferId) => request(client, { type: 'file-upload-cancel', transferId }, isTransferReply)

const sha256Of = (buffer) => crypto.createHash('sha256').update(buffer).digest('hex')

function stagingFiles(root) {
  const found = []
  const walk = (dir) => {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      if (entry.isSymbolicLink()) continue
      const full = path.join(dir, entry.name)
      if (entry.isDirectory()) walk(full)
      else if (entry.name.endsWith('.part')) found.push(path.relative(root, full))
    }
  }
  walk(root)
  return found
}

// Multi-chunk upload of a new file: every chunk is acknowledged with the next
// expected offset, the eof frame commits, and the workspace keeps no .part file.
async function chunkedUpload(client, fileName, bytes, chunkBytes) {
  const opened = await openUpload(client, { requestId: `open-${fileName}`, path: fileName, size: bytes.length, sha256: sha256Of(bytes) })
  assert.equal(opened.kind, 'file-upload-opened')
  const acks = []
  let offset = 0
  let committed = null
  while (offset < bytes.length) {
    const chunk = bytes.subarray(offset, Math.min(offset + chunkBytes, bytes.length))
    const eof = offset + chunk.length === bytes.length
    const reply = await writeUpload(client, opened.transferId, offset, chunk,
      eof ? { eof: true, sha256: sha256Of(bytes) } : {})
    if (eof) {
      committed = reply
      break
    }
    assert.equal(reply.kind, 'file-upload-ack')
    acks.push(reply)
    offset = reply.offset
  }
  return { opened, acks, committed }
}

;(async () => {
  const main = await startGateway({ fileUploadChunkMaxBytes: 64 * 1024 })
  const client = await connect(main.url)
  assert.equal(client.hello.capabilities.includes('file-uploads'), true)

  // 1. Small file in several chunks: bytes and SHA-256 match the source, no residue.
  {
    const bytes = Buffer.alloc(150 * 1024)
    for (let index = 0; index < bytes.length; index += 1) bytes[index] = (index * 31 + 7) & 0xff
    const { opened, acks, committed } = await chunkedUpload(client, 'nested/report.bin', bytes, 64 * 1024)
    assert.equal(opened.sessionId, SESSION_ID)
    assert.equal(opened.path, 'nested/report.bin')
    assert.equal(opened.size, bytes.length)
    assert.deepEqual(acks.map((ack) => ack.offset), [64 * 1024, 128 * 1024])
    assert.equal(committed.kind, 'file-upload-committed')
    assert.equal(committed.transferId, opened.transferId)
    assert.equal(committed.path, 'nested/report.bin')
    assert.equal(committed.size, bytes.length)
    assert.equal(committed.sha256, sha256Of(bytes))
    const stored = fs.readFileSync(path.join(workspace, 'nested', 'report.bin'))
    assert.equal(stored.equals(bytes), true)
    assert.equal(sha256Of(stored), sha256Of(bytes))
    assert.deepEqual(stagingFiles(workspace), [])
    console.log('PASS 1: multi-chunk upload lands byte-identical with no staging residue')
  }

  // 2. Offset must equal the bytes the server already holds.
  {
    const opened = await openUpload(client, { requestId: 'offset-1', path: 'nested/offset.bin', size: 8 })
    assert.equal(opened.kind, 'file-upload-opened')
    const reply = await writeUpload(client, opened.transferId, 1, Buffer.from('abcdefgh'))
    assert.equal(reply.kind, 'error')
    assert.equal(reply.code, 'file-transfer-offset')
    assert.equal(reply.requestType, 'file-upload-write')
    assert.equal(reply.sessionId, SESSION_ID)
    assert.equal((await cancelUpload(client, opened.transferId)).kind, 'file-upload-cancelled')
    assert.equal(fs.existsSync(path.join(workspace, 'nested', 'offset.bin')), false)
    console.log('PASS 2: mismatched offset is rejected with file-transfer-offset')
  }

  // 2b. Two writes racing the same transfer: the loser is busy or stale, never both accepted.
  {
    const opened = await openUpload(client, { requestId: 'busy-1', path: 'nested/busy.bin', size: 64 * 1024 })
    const payload = { type: 'file-upload-write', transferId: opened.transferId, offset: 0, data: Buffer.alloc(64 * 1024, 0x11).toString('base64') }
    const before = client.frames.length
    client.ws.send(JSON.stringify(payload))
    client.ws.send(JSON.stringify(payload))
    assert.equal(await waitFor(() => client.frames.slice(before).filter(isTransferReply).length >= 2), true)
    const replies = client.frames.slice(before).filter(isTransferReply)
    const errors = replies.filter((frame) => frame.kind === 'error')
    assert.equal(errors.length, 1)
    assert.equal(['file-transfer-busy', 'file-transfer-offset'].includes(errors[0].code), true, `unexpected ${errors[0].code}`)
    assert.equal((await cancelUpload(client, opened.transferId)).kind, 'file-upload-cancelled')
    console.log(`PASS 2b: concurrent write rejected with ${errors[0].code}`)
  }

  // 3. Path sandbox: parent traversal and a symlinked directory that leaves the cwd.
  {
    const escape = await openUpload(client, { requestId: 'escape-1', path: '../outside.bin', size: 4 })
    assert.equal(escape.kind, 'error')
    assert.equal(escape.code, 'bad-request')
    assert.equal(escape.requestType, 'file-upload-open')
    const absolute = await openUpload(client, { requestId: 'escape-2', path: path.join(outside, 'absolute.bin'), size: 4 })
    assert.equal(absolute.kind, 'error')
    assert.equal(absolute.code, 'bad-request')
    const symlinked = await openUpload(client, { requestId: 'escape-3', path: 'escape-dir/secret.bin', size: 4 })
    assert.equal(symlinked.kind, 'error')
    assert.equal(symlinked.code, 'file-not-allowed')
    assert.equal(fs.readFileSync(path.join(outside, 'secret.bin'), 'utf8'), 'outside the session workspace\n')
    assert.deepEqual(stagingFiles(workspace), [])
    assert.deepEqual(stagingFiles(outside), [])
    console.log('PASS 3: traversal, absolute path and escaping symlink are rejected')
  }

  // 4. Declared size above the upload limit.
  {
    const limited = await startGateway({ fileUploadMaxBytes: 1024 * 1024 })
    const limitedClient = await connect(limited.url)
    const reply = await openUpload(limitedClient, { requestId: 'too-large-1', path: 'huge.bin', size: 2 * 1024 * 1024 })
    assert.equal(reply.kind, 'error')
    assert.equal(reply.code, 'file-too-large')
    assert.equal(reply.requestType, 'file-upload-open')
    assert.equal(fs.existsSync(path.join(workspace, 'huge.bin')), false)
    limitedClient.ws.close()
    await once(limitedClient.ws, 'close')
    await limited.close()
    console.log('PASS 4: declared size above fileUploadMaxBytes is file-too-large')
  }

  // 4b. One frame above fileUploadChunkMaxBytes is rejected without touching the file.
  {
    const opened = await openUpload(client, { requestId: 'chunk-1', path: 'nested/chunk.bin', size: 128 * 1024 })
    assert.equal(opened.kind, 'file-upload-opened')
    const reply = await writeUpload(client, opened.transferId, 0, Buffer.alloc(128 * 1024, 0x22))
    assert.equal(reply.kind, 'error')
    assert.equal(reply.code, 'file-too-large')
    assert.equal((await cancelUpload(client, opened.transferId)).kind, 'file-upload-cancelled')
    assert.deepEqual(stagingFiles(workspace), [])
    console.log('PASS 4b: oversized chunk is file-too-large')
  }

  // 5. Existing destination: refused without overwrite:true, replaced with it.
  {
    const refused = await openUpload(client, { requestId: 'exists-1', path: 'nested/existing.txt', size: 4 })
    assert.equal(refused.kind, 'error')
    assert.equal(refused.code, 'file-exists')
    assert.equal(fs.readFileSync(path.join(workspace, 'nested', 'existing.txt'), 'utf8'), 'original content\n')

    const replacement = Buffer.from('replaced content\n')
    const opened = await openUpload(client, { requestId: 'exists-2', path: 'nested/existing.txt', size: replacement.length, overwrite: true })
    assert.equal(opened.kind, 'file-upload-opened')
    const eof = await writeUpload(client, opened.transferId, 0, replacement, { eof: true, sha256: sha256Of(replacement) })
    assert.equal(eof.kind, 'file-upload-committed')
    assert.equal(fs.readFileSync(path.join(workspace, 'nested', 'existing.txt'), 'utf8'), 'replaced content\n')
    assert.deepEqual(stagingFiles(workspace), [])
    console.log('PASS 5: file-exists without overwrite, overwrite:true replaces the file')
  }

  // 6. SHA-256 mismatch: no target file for a new path, untouched original for overwrite.
  {
    const bytes = Buffer.from('integrity checked payload\n')
    const opened = await openUpload(client, {
      requestId: 'integrity-1', path: 'nested/integrity.bin', size: bytes.length, sha256: sha256Of(Buffer.from('other bytes')),
    })
    assert.equal(opened.kind, 'file-upload-opened')
    const reply = await writeUpload(client, opened.transferId, 0, bytes, { eof: true })
    assert.equal(reply.kind, 'error')
    assert.equal(reply.code, 'file-integrity')
    assert.equal(reply.requestType, 'file-upload-write')
    assert.equal(fs.existsSync(path.join(workspace, 'nested', 'integrity.bin')), false)
    assert.deepEqual(stagingFiles(workspace), [])

    const guard = await openUpload(client, {
      requestId: 'integrity-2', path: 'nested/existing.txt', size: bytes.length, overwrite: true,
    })
    const damaged = await writeUpload(client, guard.transferId, 0, bytes, { eof: true, sha256: sha256Of(Buffer.from('wrong digest')) })
    assert.equal(damaged.kind, 'error')
    assert.equal(damaged.code, 'file-integrity')
    assert.equal(fs.readFileSync(path.join(workspace, 'nested', 'existing.txt'), 'utf8'), 'replaced content\n')
    assert.deepEqual(stagingFiles(workspace), [])
    console.log('PASS 6: SHA-256 mismatch cleans the staging file and leaves the destination intact')
  }

  // 7. Cancelling mid-transfer removes the staging file.
  {
    const opened = await openUpload(client, { requestId: 'cancel-1', path: 'nested/cancelled.bin', size: 128 * 1024 })
    assert.equal(opened.kind, 'file-upload-opened')
    const ack = await writeUpload(client, opened.transferId, 0, Buffer.alloc(64 * 1024, 0x33))
    assert.equal(ack.kind, 'file-upload-ack')
    assert.deepEqual(stagingFiles(workspace), [`nested/cancelled.bin.dsh-upload-${opened.transferId}.part`])
    const cancelled = await cancelUpload(client, opened.transferId)
    assert.equal(cancelled.kind, 'file-upload-cancelled')
    assert.equal(cancelled.transferId, opened.transferId)
    assert.deepEqual(stagingFiles(workspace), [])
    assert.equal(fs.existsSync(path.join(workspace, 'nested', 'cancelled.bin')), false)
    const afterCancel = await writeUpload(client, opened.transferId, ack.offset, Buffer.alloc(64 * 1024, 0x33))
    assert.equal(afterCancel.kind, 'error')
    assert.equal(afterCancel.code, 'file-transfer-not-found')
    console.log('PASS 7: cancel deletes the staging file and retires the transfer')
  }

  // 8. EOF before the declared size is complete, and createParents behaviour.
  {
    const opened = await openUpload(client, { requestId: 'short-1', path: 'nested/short.bin', size: 16 })
    const short = await writeUpload(client, opened.transferId, 0, Buffer.from('short'), { eof: true, sha256: sha256Of(Buffer.from('short')) })
    assert.equal(short.kind, 'error')
    assert.equal(short.code, 'file-transfer-size')
    assert.deepEqual(stagingFiles(workspace), [])
    assert.equal((await writeUpload(client, opened.transferId, 5, Buffer.from('short'))).code, 'file-transfer-not-found')

    const missingParent = await openUpload(client, { requestId: 'parents-1', path: 'made/up/deep/file.bin', size: 3 })
    assert.equal(missingParent.kind, 'error')
    assert.equal(missingParent.code, 'file-not-found')
    const created = await openUpload(client, { requestId: 'parents-2', path: 'made/up/deep/file.bin', size: 3, createParents: true })
    assert.equal(created.kind, 'file-upload-opened')
    const committed = await writeUpload(client, created.transferId, 0, Buffer.from('abc'), { eof: true })
    assert.equal(committed.kind, 'file-upload-committed')
    assert.equal(fs.readFileSync(path.join(workspace, 'made', 'up', 'deep', 'file.bin'), 'utf8'), 'abc')
    console.log('PASS 8: incomplete eof is file-transfer-size; createParents creates the chain')
  }

  // 9. The switch disables all three frames and drops the capability.
  {
    const disabled = await startGateway({ fileUploadsEnabled: false })
    const disabledClient = await connect(disabled.url)
    assert.equal(disabledClient.hello.capabilities.includes('file-uploads'), false)
    assert.equal(disabledClient.hello.capabilities.includes('file-downloads'), true)
    for (const frame of [
      { type: 'file-upload-open', requestId: 'off-1', sessionId: SESSION_ID, path: 'nested/off.bin', size: 3 },
      { type: 'file-upload-write', transferId: 'whatever', offset: 0, data: 'YWJj' },
      { type: 'file-upload-cancel', transferId: 'whatever' },
    ]) {
      const reply = await request(disabledClient, frame, (frame0) => frame0.kind === 'error')
      assert.equal(reply.kind, 'error')
      assert.equal(reply.code, 'file-upload-disabled')
      assert.equal(reply.requestType, frame.type)
    }
    assert.equal(fs.existsSync(path.join(workspace, 'nested', 'off.bin')), false)
    disabledClient.ws.close()
    await once(disabledClient.ws, 'close')
    await disabled.close()
    console.log('PASS 9: fileUploadsEnabled:false rejects every upload frame')
  }

  // 10. root:'temp' writes into <fileUploadTempDirectory>/<UTC date>/<name>, never
  // into the session workspace, and both replies carry the absolute path.
  {
    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-mobile-temp-upload-'))
    const tempGateway = await startGateway({ fileUploadTempDirectory: tempDir })
    const tempClient = await connect(tempGateway.url)
    const day = new Date().toISOString().slice(0, 10)
    const bytes = Buffer.from('temp upload payload\n'.repeat(64))
    const requestFrame = {
      type: 'file-upload-open', requestId: 'temp-1', sessionId: SESSION_ID,
      path: 'drop.log', size: bytes.length, sha256: sha256Of(bytes), root: 'temp',
    }
    const opened = await openUpload(tempClient, {
      requestId: 'temp-1', path: 'drop.log', size: bytes.length, sha256: sha256Of(bytes), root: 'temp',
    })
    assert.equal(opened.kind, 'file-upload-opened')
    assert.equal(opened.root, 'temp')
    assert.equal(opened.sessionId, SESSION_ID)
    assert.equal(opened.path, `${day}/drop.log`)
    assert.equal(opened.absolutePath, path.join(tempDir, day, 'drop.log'))
    assert.equal(path.isAbsolute(opened.absolutePath), true)
    assert.equal(opened.size, bytes.length)
    assert.equal(fs.statSync(path.dirname(opened.absolutePath)).isDirectory(), true, 'the UTC date bucket is created at open')

    const committed = await writeUpload(tempClient, opened.transferId, 0, bytes, { eof: true, sha256: sha256Of(bytes) })
    assert.equal(committed.kind, 'file-upload-committed')
    assert.equal(committed.root, 'temp')
    assert.equal(committed.path, `${day}/drop.log`)
    assert.equal(committed.absolutePath, opened.absolutePath)
    assert.equal(committed.size, bytes.length)
    assert.equal(committed.sha256, sha256Of(bytes))
    const stored = fs.readFileSync(committed.absolutePath)
    assert.equal(stored.equals(bytes), true)
    assert.equal(sha256Of(stored), sha256Of(bytes))
    assert.deepEqual(stagingFiles(tempDir), [])
    assert.equal(fs.existsSync(path.join(workspace, 'drop.log')), false, 'temp uploads must not enter the project directory')
    console.log(`temp upload frames: ${JSON.stringify({
      request: requestFrame,
      opened,
      committed,
    })}`)
    console.log('PASS 10: root:"temp" upload lands in <tempDir>/<date>/<name> with a matching absolutePath')

    // 11. root:'temp' accepts a bare file name only.
    for (const badPath of ['../escape.log', 'nested/escape.log', `nested${path.sep}escape.log`, '/etc/passwd', '..\\escape.log', '  ..  ', '', '   ']) {
      const reply = await openUpload(tempClient, { requestId: 'temp-bad', path: badPath, size: 4, root: 'temp' })
      assert.equal(reply.kind, 'error', `path ${JSON.stringify(badPath)} must be rejected`)
      assert.equal(reply.code, 'bad-request')
      assert.equal(reply.requestType, 'file-upload-open')
      assert.equal(reply.sessionId, SESSION_ID)
    }
    const badRoot = await openUpload(tempClient, { requestId: 'temp-root', path: 'ok.log', size: 4, root: 'elsewhere' })
    assert.equal(badRoot.kind, 'error')
    assert.equal(badRoot.code, 'bad-request')
    assert.deepEqual(stagingFiles(tempDir), [])
    console.log('PASS 11: root:"temp" rejects traversal, separators, absolute paths and unknown roots')

    // 12. Regression: an omitted root still targets the session workspace.
    {
      const body = Buffer.from('abc')
      const openedDefault = await openUpload(tempClient, { requestId: 'default-root-1', path: 'default-root.txt', size: body.length })
      assert.equal(openedDefault.kind, 'file-upload-opened')
      assert.equal(openedDefault.root, 'workspace')
      assert.equal(openedDefault.path, 'default-root.txt')
      assert.equal(openedDefault.absolutePath, path.join(workspace, 'default-root.txt'))
      const committedDefault = await writeUpload(tempClient, openedDefault.transferId, 0, body, { eof: true })
      assert.equal(committedDefault.kind, 'file-upload-committed')
      assert.equal(committedDefault.root, 'workspace')
      assert.equal(committedDefault.absolutePath, path.join(workspace, 'default-root.txt'))
      assert.equal(fs.readFileSync(path.join(workspace, 'default-root.txt'), 'utf8'), 'abc')
      assert.equal(fs.existsSync(path.join(tempDir, day, 'default-root.txt')), false)
      const explicit = await openUpload(tempClient, { requestId: 'explicit-root-1', path: 'explicit-root.txt', size: body.length, root: 'workspace' })
      assert.equal(explicit.root, 'workspace')
      assert.equal(explicit.absolutePath, path.join(workspace, 'explicit-root.txt'))
      assert.equal((await cancelUpload(tempClient, explicit.transferId)).kind, 'file-upload-cancelled')
      console.log('PASS 12: omitted root keeps writing into the session workspace')
    }

    // 13. Temp sandbox: a symlinked leaf that leaves the temp directory is refused.
    {
      const outsideDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-mobile-temp-outside-'))
      fs.writeFileSync(path.join(outsideDir, 'target.log'), 'outside the temp directory\n')
      fs.symlinkSync(path.join(outsideDir, 'target.log'), path.join(tempDir, day, 'linked.log'))
      const reply = await openUpload(tempClient, { requestId: 'temp-link-1', path: 'linked.log', size: 4, overwrite: true, root: 'temp' })
      assert.equal(reply.kind, 'error')
      assert.equal(reply.code, 'file-not-allowed')
      assert.equal(fs.readFileSync(path.join(outsideDir, 'target.log'), 'utf8'), 'outside the temp directory\n')
      assert.deepEqual(stagingFiles(outsideDir), [])
      tempClient.ws.close()
      await once(tempClient.ws, 'close')
      await tempGateway.close()
      fs.rmSync(tempDir, { recursive: true, force: true })
      fs.rmSync(outsideDir, { recursive: true, force: true })
      console.log('PASS 13: a symlink escaping the temp directory is file-not-allowed')
    }
  }

  // 14. Retention sweep: files and staging leftovers past their age are removed,
  // fresh files and non-upload content are kept.
  {
    const day = new Date().toISOString().slice(0, 10)
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-mobile-temp-sweep-'))
    const bucket = path.join(root, day)
    fs.mkdirSync(bucket, { recursive: true })
    const oldFile = path.join(bucket, 'old.log')
    const freshFile = path.join(bucket, 'fresh.log')
    const stalePart = path.join(bucket, 'interrupted.log.dsh-upload-abc.part')
    const emptyOld = path.join(root, '2026-01-01')
    fs.writeFileSync(oldFile, 'old\n')
    fs.writeFileSync(freshFile, 'fresh\n')
    fs.writeFileSync(stalePart, 'partial')
    fs.mkdirSync(emptyOld)
    const eightDaysAgo = new Date(Date.now() - 8 * 24 * 60 * 60 * 1000)
    fs.utimesSync(oldFile, eightDaysAgo, eightDaysAgo)
    fs.utimesSync(stalePart, eightDaysAgo, eightDaysAgo)
    fs.utimesSync(emptyOld, eightDaysAgo, eightDaysAgo)

    const swept = await sweepTempUploadDirectory(root, 7)
    assert.equal(swept.skipped, false)
    assert.equal(swept.removed, 2, `expected the 8-day-old file and stale .part to go, removed=${swept.removed}`)
    assert.equal(swept.kept, 1, `expected only the fresh file to stay, kept=${swept.kept}`)
    assert.equal(fs.existsSync(oldFile), false)
    assert.equal(fs.existsSync(stalePart), false)
    assert.equal(fs.existsSync(freshFile), true)
    assert.equal(fs.existsSync(emptyOld), false, 'an aged empty directory is pruned')
    assert.deepEqual(stagingFiles(root), [])

    // A directory emptied by the sweep itself disappears with its last file.
    // 注意：被"外部"清空的日期目录不会立刻删——正在进行的上传可能刚建好目录还没落文件，
    // 立刻 rmdir 会抢掉它的目录。所以空目录要等自身 mtime 也超过保留期才删。
    fs.rmSync(freshFile)
    const stillThere = await sweepTempUploadDirectory(root, 7)
    assert.equal(stillThere.removed, 0)
    assert.equal(fs.existsSync(bucket), true, 'a freshly emptied bucket stays for in-flight uploads')
    fs.utimesSync(bucket, eightDaysAgo, eightDaysAgo)
    const cascaded = await sweepTempUploadDirectory(root, 7)
    assert.equal(cascaded.removed, 0)
    assert.equal(fs.existsSync(bucket), false, 'an aged, emptied date bucket is pruned')
    assert.equal(fs.existsSync(root), true, 'the temp root itself is never removed')

    // retention 0 disables cleanup entirely: nothing is deleted, ever.
    fs.mkdirSync(bucket, { recursive: true })
    const keptOld = path.join(bucket, 'kept-old.log')
    fs.writeFileSync(keptOld, 'kept\n')
    fs.utimesSync(keptOld, eightDaysAgo, eightDaysAgo)
    const disabled = await sweepTempUploadDirectory(root, 0)
    assert.equal(disabled.skipped, true)
    assert.equal(disabled.removed, 0)
    assert.equal(disabled.kept, 0)
    assert.equal(fs.existsSync(keptOld), true)
    fs.rmSync(root, { recursive: true, force: true })
    console.log('PASS 14: the sweep removes 8-day-old files and stale .part files, and keeps everything when retention is 0')
  }

  // 15. End to end: the plugin sweeps at startup, and fileUploadTempRetentionDays:0
  // keeps the same directory untouched.
  {
    const sweepDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-mobile-temp-startup-'))
    const bucket = path.join(sweepDir, new Date().toISOString().slice(0, 10))
    fs.mkdirSync(bucket, { recursive: true })
    const stale = path.join(bucket, 'stale.log')
    const fresh = path.join(bucket, 'fresh.log')
    fs.writeFileSync(stale, 'stale\n')
    fs.writeFileSync(fresh, 'fresh\n')
    const eightDaysAgo = new Date(Date.now() - 8 * 24 * 60 * 60 * 1000)
    fs.utimesSync(stale, eightDaysAgo, eightDaysAgo)

    const keepGateway = await startGateway({ fileUploadTempDirectory: sweepDir, fileUploadTempRetentionDays: 0 })
    await new Promise((resolve) => setTimeout(resolve, 300))
    assert.equal(fs.existsSync(stale), true, 'retention 0 must not delete anything')
    keepGateway.close()

    const sweepGateway = await startGateway({ fileUploadTempDirectory: sweepDir })
    assert.equal(await waitFor(() => !fs.existsSync(stale)), true, 'the startup sweep must remove the stale temp upload')
    assert.equal(fs.existsSync(fresh), true, 'the fresh temp upload stays')
    sweepGateway.close()
    fs.rmSync(sweepDir, { recursive: true, force: true })
    console.log('PASS 15: the startup sweep honours fileUploadTempRetentionDays')
  }

  client.ws.close()
  await once(client.ws, 'close')
  await main.close()
  fs.rmSync(workspace, { recursive: true, force: true })
  fs.rmSync(outside, { recursive: true, force: true })
  console.log('FILE UPLOAD TESTS PASSED')
})().catch((error) => {
  console.error(error)
  process.exit(1)
})
