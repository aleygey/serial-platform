// Offline application interaction checks using Electron's real input dispatch.
const { app, BrowserWindow, Menu } = require('electron')
const assert = require('node:assert/strict')
const { mkdtempSync, mkdirSync, writeFileSync } = require('node:fs')
const { join } = require('node:path')
const { tmpdir } = require('node:os')

app.setPath('userData', mkdtempSync(join(tmpdir(), 'serial-desktop-ui-qa-')))
const pause = (ms) => new Promise((resolve) => setTimeout(resolve, ms))
let window
const js = (code) => window.webContents.executeJavaScript(code)
async function until(code) {
  for (let attempt = 0; attempt < 100; attempt++) {
    if (await js(code)) return
    await pause(20)
  }
  process.stderr.write(JSON.stringify(await js(`({active:document.activeElement?.outerHTML, focus:document.hasFocus(), bar:document.querySelector('.command-bar')?.className, start:document.activeElement?.selectionStart, end:document.activeElement?.selectionEnd, ghost:document.querySelector('.command-ghost')?.textContent, text:document.body.textContent.slice(-600)})`)) + '\n')
  await screenshot('interaction-failure.png')
  throw new Error(`UI condition failed: ${code}`)
}
async function key(keyCode, modifiers = []) {
  window.webContents.sendInputEvent({ type: 'keyDown', keyCode, modifiers })
  window.webContents.sendInputEvent({ type: 'keyUp', keyCode, modifiers })
  await pause(35)
}
async function fill(value, start = value.length, end = start) {
  await js(`(() => {
    const input = document.querySelector('[aria-label="输入串口命令"]'); input.focus();
    Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set.call(input, ${JSON.stringify(value)});
    input.dispatchEvent(new Event('input', { bubbles: true }));
    input.setSelectionRange(${start}, ${end}); document.dispatchEvent(new Event('selectionchange'));
  })()`)
  await pause(60)
}
async function screenshot(name) {
  await js(`new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)))`)
  await pause(150)
  const path = join(__dirname, '../qa')
  mkdirSync(path, { recursive: true })
  writeFileSync(join(path, name), (await window.capturePage()).toPNG())
}

app.whenReady().then(async () => {
  Menu.setApplicationMenu(null)
  window = new BrowserWindow({ width: 1480, height: 920, show: true, webPreferences: { contextIsolation: true, nodeIntegration: false, sandbox: true } })
  await window.loadFile(join(__dirname, '../out/renderer/index.html'), { query: { qa: '1', theme: 'dark' } })
  window.focus()
  window.webContents.focus()
  await until(`!!document.querySelector('[aria-label="输入串口命令"]')`)
  await js(`window.qaCommands = []; window.qaSignals = []; window.serial.sendCommand = async (port, command) => { window.qaCommands.push({port, command}); return {status:'accepted'} }; window.serial.sendSignal = async (port, signal) => { window.qaSignals.push({port, signal}); return {status:'accepted'} }; true`)
  await fill('cat')
  await until(`document.querySelector('.command-ghost').textContent === 'cat /etc/version'`)
  await screenshot('input-autosuggestion-dark.png')
  await key('Right')
  assert.equal(await js(`document.querySelector('[aria-label="输入串口命令"]').value`), 'cat /etc/version')
  assert.equal(await js('window.qaCommands.length'), 0)
  await fill('cat', 1)
  await key('Right')
  assert.deepEqual(await js(`({value: document.querySelector('[aria-label="输入串口命令"]').value, cursor: document.querySelector('[aria-label="输入串口命令"]').selectionStart})`), { value: 'cat', cursor: 2 })
  await fill('cat')
  await key('Enter')
  assert.equal(await js('window.qaCommands.at(-1).command'), 'cat')
  await until(`document.querySelector('[aria-label="输入串口命令"]').value === ''`)
  await fill('cat')
  await key('Tab')
  await until(`!!document.querySelector('[role="listbox"]')`)
  await screenshot('input-history-menu-dark.png')
  await key('Down')
  await key('Enter')
  assert.equal(await js('window.qaCommands.length'), 1)
  assert.equal(await js(`document.querySelector('[aria-label="输入串口命令"]').value`), 'cat /proc/meminfo')
  await fill('my unfinished draft', 3)
  await key('Up')
  await key('Down')
  assert.deepEqual(await js(`({value: document.querySelector('[aria-label="输入串口命令"]').value, cursor: document.querySelector('[aria-label="输入串口命令"]').selectionStart})`), { value: 'my unfinished draft', cursor: 3 })
  await key('D', ['control'])
  assert.deepEqual(await js('window.qaSignals'), [{ port: 'COM6', signal: 'ctrl_d' }])
  assert.equal(await js(`document.querySelector('[aria-label="输入串口命令"]').value`), 'my unfinished draft')
  await key('R', ['control'])
  await until(`!!document.querySelector('[aria-label="搜索人工命令历史"]')`)
  await key('Escape')
  assert.equal(await js(`document.querySelector('[aria-label="输入串口命令"]').selectionStart`), 3)
  await js(`window.qaMacroDefinitions = {}; window.qaMacroSaves = []; window.qaMacroRuns = [];
    window.serial.listMacros = async query => { const all = Object.values(window.qaMacroDefinitions).filter(item => query.include_drafts || item.shared); return {catalog_revision:window.qaMacroSaves.length, macros:all, total:all.length, definition:window.qaMacroDefinitions[query.id]}; };
    window.serial.saveMacro = async request => { window.qaMacroSaves.push(request); const definition = {...request, language_version:1, revision:(request.expected_revision || 0)+1, updated_at_ns:0}; window.qaMacroDefinitions[request.id] = definition; return {catalog_revision:window.qaMacroSaves.length, definition}; };
    window.serial.runMacro = async (port, spec) => { window.qaMacroRuns.push({port,spec}); return {id:'qa-execution', port, description:'mock only', status:'succeeded', line:1, writes:1, outcome_uncertain:false}; }; true`)
  await js(`Array.from(document.querySelectorAll('.view-tabs button')).find(button => button.textContent.includes('宏')).click()`)
  await until(`!!document.querySelector('.macro-page')`)
  await js(`Array.from(document.querySelectorAll('button')).find(button => button.textContent === 'U-Boot 模板').click()`)
  await until(`document.querySelector('[aria-label="宏 ID"]').value === 'enter_uboot'`)
  await screenshot('macro-editor-dark.png')
  await js(`document.documentElement.dataset.theme = 'light'`)
  await screenshot('macro-editor-light.png')
  await js(`document.querySelector('.macro-editor-panel button[type="submit"]').click()`)
  await until(`!!document.querySelector('.macro-notice')`)
  assert.equal(await js(`window.qaMacroSaves[0].shared`), false)
  assert.equal(await js(`document.querySelectorAll('.macro-list button').length`), 0)
  await js(`document.querySelector('.macro-drafts-toggle input').click()`)
  await until(`document.querySelectorAll('.macro-list button').length === 1`)
  await js(`document.querySelector('.macro-run-actions button').click()`)
  await until(`window.qaMacroRuns.length === 1`)
  assert.deepEqual(await js(`window.qaMacroRuns[0].spec`), {macro_id:'enter_uboot', revision:1, args:{interval_ms:50}, timeout_seconds:30})
  await js(`(() => { const input = document.querySelector('.macro-script'); Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, 'value').set.call(input, 'cmd("version");'); input.dispatchEvent(new Event('input', {bubbles:true})); })()`)
  await until(`document.querySelector('.macro-run-actions button').disabled`)
  await js(`document.querySelector('.macro-editor-panel button[type="submit"]').click()`)
  await until(`window.qaMacroSaves.length === 2`)
  assert.equal(await js(`window.qaMacroSaves[1].expected_revision`), 1)
  await js(`document.querySelector('.macro-editor-panel').scrollTop = 1000`)
  await screenshot('macro-run-result-light.png')
  process.stdout.write('PASS: real Electron ghost/Right/Enter/Tab/history/Ctrl-D/Ctrl-R, draft-only macros, versioned save/run, and dark/light macro views\n')
  app.exit(0)
}).catch((error) => { process.stderr.write(`${error.stack}\n`); app.exit(1) })
