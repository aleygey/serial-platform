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
  process.stderr.write(`Timed out waiting for ${code}\n`)
  process.stderr.write(JSON.stringify(await js(`({active:document.activeElement?.outerHTML, focus:document.hasFocus(), bar:document.querySelector('.command-bar')?.className, start:document.activeElement?.selectionStart, end:document.activeElement?.selectionEnd, ghost:document.querySelector('.command-ghost')?.textContent, text:document.body.textContent.slice(-600)})`)) + '\n')
  await screenshot('interaction-failure.png')
  throw new Error(`UI condition failed: ${code}`)
}
async function key(keyCode, modifiers = []) {
  window.webContents.sendInputEvent({ type: 'keyDown', keyCode, modifiers })
  window.webContents.sendInputEvent({ type: 'keyUp', keyCode, modifiers })
  await pause(35)
}

async function clickAt(selector) {
  const point = await js(`(() => {const el=document.querySelector(${JSON.stringify(selector)}); const r=el.getBoundingClientRect();
    const x=Math.round(r.left+r.width/2), y=Math.round(r.top+r.height/2);
    if (!el.contains(document.elementFromPoint(x,y))) throw Error('Pointer cannot reach '+el.outerHTML);
    return {x,y}})()`)
  for (const type of ['mouseMove','mouseDown','mouseUp']) {
    window.webContents.sendInputEvent({type, button:'left', clickCount:1, ...point})
  }
}
async function checkReleasePreferences() {
  await js(`window.qaReleaseChecks=0; window.qaReleaseResult='offline';
    window.desktopUpdates={installedVersion:'1.0.0', install:async version=>{window.qaInstalled=version; throw Error('fixture install failure')}, check:async()=>{
      window.qaReleaseChecks++;
      if(window.qaReleaseResult==='offline') throw Error('offline');
      return window.qaReleaseResult==='current' ? null : {version:'1.1.0',installable:true};
    }}; window.serial.savePreferences({autoCheckUpdates:false})`)
  const check = "document.querySelector('.release-check button')"
  const toggle = "document.querySelector('[aria-label=\"自动检查更新\"]')"
  await until(`!!${check} && !${toggle}.checked`)
  await js(`${check}.click()`)
  await until(`document.querySelector('.release-check [role="status"]').textContent.includes('重试')`)
  await js(`window.qaReleaseResult='current'; ${check}.click()`)
  await until(`document.querySelector('.release-check [role="status"]').textContent.includes('已是最新')`)
  assert.equal(await js('window.qaReleaseChecks'), 2)
  await js(`window.qaReleaseResult='available'; ${toggle}.click()`)
  await until(`${toggle}.checked`)
  await pause(8100)
  await until(`document.querySelector('.window-update')?.textContent.includes('1.1.0') && document.querySelector('.release-download')?.textContent.includes('更新并重启')`)
  assert.equal(await js('window.qaReleaseChecks'), 3)
  await clickAt('.release-download')
  await until(`window.qaInstalled==='1.1.0' && document.querySelector('.release-check [role="status"]').textContent.includes('更新失败')`)
  await js(`${toggle}.click()`)
  await until(`!${toggle}.checked`)
  assert.equal(await js('(async()=>(await window.serial.bootstrap()).preferences.autoCheckUpdates)()'), false)
  await js('delete window.desktopUpdates; window.serial.savePreferences({})')
}
async function dragFromRail(port, { cancel = false } = {}) {
  window.focus()
  window.webContents.focus()
  const { from, left, right } = await js(`(() => {
    const row = [...document.querySelectorAll('.device-choice')].find(item => item.querySelector('strong').textContent === ${JSON.stringify(port)})
    const a = row.getBoundingClientRect(), b = document.querySelector('.pane-deck').getBoundingClientRect()
    const y = Math.round(b.top + 72)
    return {from:{x:Math.round(a.left + 18),y:Math.round(a.top + 14)},left:{x:Math.round(b.left+35),y},right:{x:Math.round(b.right-35),y}}
  })()`)
  window.webContents.sendInputEvent({type:'mouseMove', ...from})
  window.webContents.sendInputEvent({type:'mouseDown',button:'left',clickCount:1,...from})
  for (const [start,end] of [[from,left],[left,right]]) {
    for (let i=1; i<=8; i++) {
      window.webContents.sendInputEvent({type:'mouseMove',modifiers:['leftButtonDown'],x:Math.round(start.x+(end.x-start.x)*i/8),y:Math.round(start.y+(end.y-start.y)*i/8)})
      await pause(20)
    }
    const side = end === left ? 'first' : 'last'
    await until(`document.querySelector('.workspace-panels > .insertion-gap:${side}-child[data-drop-preview]')?.clientWidth > document.querySelector('.pane-deck').clientWidth * 0.4`)
  }
  if (cancel) await key('Escape')
  window.webContents.sendInputEvent({type:'mouseUp',button:'left',clickCount:1,...right})
  await pause(80)
}

async function dragBetween(fromSelector, toSelector, fraction = 0.25) {
  const [from, to] = await js(`[${JSON.stringify(fromSelector)}, ${JSON.stringify(toSelector)}].map((selector,index) => {
    const rect = document.querySelector(selector).getBoundingClientRect()
    return { x: Math.round(rect.left + rect.width * (index ? ${fraction} : 0.5)), y: Math.round(rect.top + rect.height / 2) }
  })`)
  const mouse = (type, point, extra = {}) => window.webContents.sendInputEvent({ type, ...point, ...extra })
  mouse('mouseMove', from)
  mouse('mouseDown', from, { button:'left', clickCount:1 })
  for (let i = 1; i <= 12; i++) {
    mouse('mouseMove', { x:Math.round(from.x + (to.x-from.x)*i/12), y:Math.round(from.y + (to.y-from.y)*i/12) }, { modifiers:['leftButtonDown'] })
    await pause(18)
  }
  mouse('mouseUp', to, { button:'left', clickCount:1 })
  await pause(180)
}

async function fill(value, start = value.length, end = start, port) {
  await js(`(() => {
    const input = document.querySelector(${JSON.stringify((port ? `[data-port="${port}"] ` : '') + '[aria-label="输入串口命令"]')}); input.focus();
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

async function checkTcpConfiguration() {
  await js(`document.querySelector('[data-slot="gooey-nav-segment"]:last-child button').click()`)
  await until(`!!document.querySelector('.settings-sections [data-id="serial"]')`)
  await js(`document.querySelector('.settings-sections [data-id="serial"]').click()`)
  await clickAt('[aria-label="串口"]')
  await clickAt('[role="option"][data-id="tcp"]')
  const address = '[aria-label="TCP 服务器地址"]'
  const set = text => js(`{const input=document.querySelector(${JSON.stringify(address)});
    Object.getOwnPropertyDescriptor(HTMLInputElement.prototype,'value').set.call(input,${JSON.stringify(text)});
    input.dispatchEvent(new Event('input',{bubbles:true}))}`)
  await set('192.168.1.252:65536')
  await until(`document.querySelector('.editor-heading .primary-button').disabled`)
  await set('192.168.1.252:10001')
  await until(`!document.querySelector('.editor-heading .primary-button').disabled && !document.querySelector('.baud-value')`)
  await clickAt('.editor-heading .primary-button')
  await until(`(async () => (await window.serial.bootstrap()).configuredPorts.some(p=>p.config.port==='192.168.1.252:10001'))()`)
}

async function checkNativeTitleBar() {
  const { installWindowControls, titleBarOptions } = await import('../src/main/window-controls.ts')
  const state = await js('window.serial.bootstrap()')
  const probe = new BrowserWindow({ width: 1100, height: 750, ...titleBarOptions,
    webPreferences: { preload: join(__dirname, '../out/preload/index.cjs'), contextIsolation: true, sandbox: true } })
  installWindowControls(probe)
  const fullScreen = () => probe.isFullScreen() || (process.platform === 'darwin' && probe.isSimpleFullScreen())
  probe.webContents.ipc.handle('serial:bootstrap', () => ({ ...state, availablePorts: [], configuredPorts: [], events: {} }))
  const inspect = code => probe.webContents.executeJavaScript(code)
  async function waitFor(label, predicate) {
    for (let count = 0; count < 100; count++) { if (await predicate()) return; await pause(50) }
    const details = probe.isDestroyed() ? 'closed' : JSON.stringify({ fullScreen: fullScreen(), minimized: probe.isMinimized(),
      controls: await inspect(`Array.from(document.querySelectorAll('.window-controls button'), b => b.getAttribute('aria-label'))`) })
    throw new Error('Native window did not ' + label + ': ' + details)
  }
  const click = label => inspect(`document.querySelector('.window-controls [aria-label="${label}"]').click()`)
  try {
    await probe.loadFile(join(__dirname, '../out/renderer/index.html'))
    probe.focus()
    await waitFor('show navigation and controls', () => inspect(`!!document.querySelector('.window-bar nav') && !!document.querySelector('.window-close')`))
    assert.deepEqual(await inspect(`(() => {
      const bar=document.querySelector('.window-bar').getBoundingClientRect(), middle=document.elementFromPoint(bar.width/2,bar.height/2)
      return [middle.className, getComputedStyle(middle).getPropertyValue('app-region'),
        getComputedStyle(document.querySelector('.window-bar nav button')).getPropertyValue('app-region'),
        getComputedStyle(document.querySelector('.window-close')).getPropertyValue('app-region')]
    })()`), ['window-drag-region', 'drag', 'no-drag', 'no-drag'])
    await click('进入全屏')
    await waitFor('enter full screen', async () => fullScreen() && await inspect(`!!document.querySelector('[aria-label="退出全屏"]')`))
    await click('退出全屏')
    await waitFor('leave full screen', async () => !fullScreen() && await inspect(`!!document.querySelector('[aria-label="进入全屏"]')`))
    await click('最小化窗口')
    await waitFor('minimize', () => probe.isMinimized())
    probe.restore()
    await waitFor('restore', () => !probe.isMinimized())
    await click('关闭窗口')
    await waitFor('close', () => probe.isDestroyed())
  } finally { if (!probe.isDestroyed()) probe.destroy() }
}

async function setAgentPhase(phase) {
  await js(`(async () => {
    const snapshot = await window.serial.bootstrap();
    const original = window.qaRunningPort ??= snapshot.configuredPorts.find(item => item.config.port === 'COM7');
    const port = {...original};
    if (${JSON.stringify(phase)} === 'awaiting-read') port.run_context = {...port.run_context, revision:2, acknowledged_revision:1};
    if (['occupied', 'idle'].includes(${JSON.stringify(phase)})) port.active_run = null;
    if (${JSON.stringify(phase)} === 'idle') port.control = null;
    snapshot.configuredPorts = snapshot.configuredPorts.map(item => item.config.port === 'COM7' ? port : item);
    await window.serial.savePreferences(snapshot.preferences);
  })()`)
  await until(`!!document.querySelector('[data-port="COM7"] .history-strip[data-phase="${phase === 'idle' ? 'idle' : 'agent'}"]')`)
}

app.whenReady().then(async () => {
  Menu.setApplicationMenu(null)
  window = new BrowserWindow({ width: 1480, height: 920, show: true, webPreferences: { contextIsolation: true, nodeIntegration: false, sandbox: true, backgroundThrottling: false } })
  await window.loadFile(join(__dirname, '../out/renderer/index.html'), { query: { qa: '1', theme: 'dark' } })
  window.focus()
  window.webContents.focus()
  await until(`!!document.querySelector('[aria-label="输入串口命令"]')`)
  if (process.argv.includes('--live-output')) {
    await dragFromRail('COM7')
    await until(`document.querySelectorAll('.port-console').length === 2`)
    await require('./qa-live-output.cjs').checkLiveOutput(window)
    app.exit(0)
    return
  }
  if (process.argv.includes('--window-controls')) {
    await checkNativeTitleBar()
    app.exit(0)
    return
  }
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
  await dragFromRail('COM7', {cancel:true})
  assert.equal(await js(`document.querySelectorAll('.port-console').length`), 1)
  await dragFromRail('COM7')
  await until(`document.querySelectorAll('.port-console').length === 2`)
  await fill('version', 7, 7, 'COM6')
  await fill('uptime', 6, 6, 'COM7')
  await until(`document.querySelector('[data-port="COM7"] .live-command')?.dataset.tone === 'working'`)
  await until(`!!document.querySelector('[data-port="COM7"] .rail-node[data-tone="working"]')`)
  await screenshot('console-history-split-dark.png')
  assert.equal(await js(`document.querySelectorAll('form.command-bar [data-slot="prompt-box"] [role="combobox"]').length`), 2)
  await dragBetween('[data-port="COM6"] [data-slot="model-picker"] button', '[data-port="COM7"] .terminal-header', 0.15)
  await until(`document.querySelector('.workspace-panels [data-port]').dataset.port === 'COM7'`)
  assert.equal(await js(`!!document.querySelector('[data-slot="model-picker-panel"]')`), false)
  await dragBetween('[data-port="COM7"] .pane-grip', '[data-port="COM6"] .terminal-header', 0.5)
  await until(`document.querySelector('.workspace-panels [data-port]').dataset.port === 'COM6'`)
  await dragBetween('[data-device-port="COM6"] .device-choice', '[data-device-port="COM7"] .device-choice')
  await until(`document.querySelector('[data-device-port]').dataset.devicePort === 'COM7'`)
  await clickAt('[aria-label="切换 COM6 机型"]')
  await until(`document.querySelector('.picker-level')?.textContent.includes('具体型号')`)
  await clickAt('[aria-label="全部机型系列"]')
  await until(`document.querySelectorAll('[role="option"][data-series]').length === 2`)
  await clickAt('[data-id="series:SHT3x"]')
  await until(`document.querySelector('.picker-level')?.textContent.includes('SHT3x')`)
  assert.equal(await js(`(async () => (await window.serial.bootstrap()).configuredPorts.find(p=>p.config.port==='COM6').config.model_family)()`), 'TL-AS7230')
  await clickAt('[role="option"]')
  await until(`document.querySelector('[aria-label="切换 COM6 机型"]').title.includes('SHT31 Sensor Board')`)
  await until(`!document.querySelector('[data-slot="model-picker-panel"]')`)
  await js(`document.querySelector('[aria-label="移动 COM7 分栏"]').focus()`)
  await key('Left')
  await until(`document.querySelector('.workspace-panels [data-port]').dataset.port === 'COM7'`)
  await key('Right')
  await until(`document.querySelector('.workspace-panels [data-port]').dataset.port === 'COM6'`)
  assert.equal(await js(`document.querySelector('[data-port="COM6"] .command-editor input').value`), 'version')
  assert.equal(await js(`document.querySelector('[data-port="COM6"] .terminal-output').textContent.includes('Sensor Board')`), false)
  assert.equal(await js(`document.querySelector('[data-port="COM7"] .history-toggle').getAttribute('aria-expanded')`), 'true')
  assert.equal(await js(`document.querySelector('[data-port="COM6"] .history-toggle').getAttribute('aria-expanded')`), 'false')
  assert.equal(await js(`document.querySelectorAll('[data-port] .agent-pane').length`), 1)
  await js(`document.querySelector('[data-port="COM7"] .history-collapse').focus()`)
  await key('Space')
  await until(`document.querySelector('[data-port="COM7"] .history-drawer').getBoundingClientRect().height < 1`)
  assert.ok(await js(`document.activeElement.matches('[data-port="COM7"] .history-toggle')`))
  assert.equal(await js(`document.querySelectorAll('.window-bar button svg').length`),3)
  await screenshot('console-split-dark.png')
  await js(`(async()=>{const {preferences}=await window.serial.bootstrap();await window.serial.savePreferences({...preferences,theme:'light'})})()`)
  await screenshot('console-split-light.png')
  await js(`(async()=>{const {preferences}=await window.serial.bootstrap();await window.serial.savePreferences({...preferences,theme:'dark'})})()`)
  await setAgentPhase('awaiting-read')
  await js(`document.querySelector('[data-port="COM7"] .orb-control').click()`)
  await until(`document.querySelector('[data-port="COM7"]').dataset.motion === 'paused'`)
  await setAgentPhase('occupied')
  assert.equal(await js(`document.querySelector('[data-port="COM7"] .command-editor input').disabled`), true)
  assert.equal(await js(`document.querySelector('[data-port="COM6"] .command-editor input').disabled`), false)
  assert.equal(await js(`document.querySelector('[data-port="COM7"] [data-slot="matrix-orb"]').dataset.state`), 'thinking')
  await setAgentPhase('idle')
  assert.equal(await js(`document.querySelector('[data-port="COM7"] [data-slot="matrix-orb"]').dataset.state`), 'idle')
  assert.equal(await js(`document.querySelector('[data-port="COM7"] .command-editor input').disabled`), false)
  await setAgentPhase('running')
  await js(`document.querySelector('[data-port="COM7"] .orb-control').click()`)
  window.webContents.debugger.attach()
  try {
    const command = '[data-port="COM7"] .live-command'
    await window.webContents.debugger.sendCommand('Emulation.setEmulatedMedia', { features: [{ name: 'prefers-reduced-motion', value: 'reduce' }] })
    await until(`matchMedia('(prefers-reduced-motion: reduce)').matches && getComputedStyle(document.querySelector(${JSON.stringify(command)}), '::after').animationName === 'none'`)
    await window.webContents.debugger.sendCommand('Emulation.setEmulatedMedia', { features: [{ name: 'prefers-reduced-motion', value: 'no-preference' }] })
    await until(`getComputedStyle(document.querySelector(${JSON.stringify(command)}), '::after').animationName === 'command-cursor'`)
  } finally {
    window.webContents.debugger.detach()
  }
  await key('F', process.platform === 'darwin' ? ['meta'] : ['control'])
  await until(`!!document.querySelector('[data-port="COM7"] .terminal-find-widget')`)
  assert.equal(await js(`!!document.querySelector('[data-port="COM6"] .terminal-find-widget')`), false)
  await key('Escape')
  await key('K', process.platform === 'darwin' ? ['meta'] : ['control'])
  assert.equal(await js(`document.activeElement.closest('[data-port]').dataset.port`), 'COM7')
  await js(`window.qaPending = []; window.serial.sendCommand = (port, command) => { window.qaCommands.push({port, command}); return new Promise(resolve => window.qaPending.push(resolve)); }; true`)
  await key('Enter')
  await until(`document.querySelector('[data-port="COM7"] .send-button').getAttribute('aria-busy') === 'true'`)
  assert.deepEqual(await js('window.qaCommands.at(-1)'), { port:'COM7', command:'uptime' })
  await key('Enter')
  await js(`document.activeElement.dispatchEvent(new KeyboardEvent('keydown', { key:'Enter', repeat:true, bubbles:true, cancelable:true }))`)
  await fill('next line', 9, 9, 'COM7')
  await key('Enter')
  assert.deepEqual(await js('window.qaCommands.slice(-4).map(x=>x.command)'), ['uptime', '', '', 'next line'])
  assert.equal(await js(`document.querySelector('[data-port="COM6"] .send-button').disabled`), false)
  await js(`document.querySelector('[aria-label="关闭 COM7 分栏"]').click()`)
  await dragFromRail('COM7')
  await until(`document.querySelector('[data-port="COM7"] .send-button').getAttribute('aria-busy') === 'true'`)
  await js(`window.qaPending.reverse().forEach(finish => finish({status:'accepted'}))`)
  await until(`document.querySelector('[data-port="COM7"] .command-editor input').value === ''`)
  assert.equal(await js(`document.querySelector('[data-port="COM6"] .command-editor input').value`), 'version')
  assert.equal(await js(`document.querySelector('[data-port="COM7"] .history-toggle').getAttribute('aria-expanded')`), 'false')
  await js(`document.querySelector('[data-port="COM7"] .history-toggle').click()`)
  await until(`!!document.querySelector('[data-port="COM7"] .agent-scroll .command-ink[data-tone="done"]')`)
  assert.equal(await js(`document.querySelectorAll('.agent-pane .command-position, .agent-pane .outline-label svg, .agent-pane [data-id="human"]').length`), 0)
  assert.equal(await js(`document.querySelector('.run-lifecycle span').textContent`), 'Run 已启动')
  await js(`document.querySelector('[aria-label="关闭 COM7 分栏"]').click()`)
  await until(`document.querySelectorAll('.port-console').length === 1`)
  assert.equal(await js(`(async () => (await window.serial.bootstrap()).configuredPorts.find(port => port.config.port === 'COM7').session_state)()`), 'online')
  assert.equal(await js(`'openAgentTask' in window.serial || !!document.querySelector('.agent-task, .agent-launch')`), false)
  assert.deepEqual(await js(`Array.from(document.querySelectorAll('.window-bar button'), b => b.textContent)`), ['控制台','宏','配置'])
  await js(`window.serial.sendCommand = async (port, command) => { window.qaCommands.push({port, command}); return {status:'accepted'} }; true`)
  await fill('  version  ')
  await js(`document.querySelector('.command-bar .send-button').click()`)
  await until(`window.qaCommands.at(-1).command === '  version  '`)
  await until(`document.querySelector('.command-editor input').value === ''`)
  await key('Enter')
  assert.deepEqual(await js('window.qaCommands.at(-1)'), { port: 'COM6', command: '' })
  await js(`document.querySelector('[data-slot="gooey-nav-segment"]:last-child button').click()`)
  await until(`!!document.querySelector('[aria-selected="true"][data-id="serial"]')`)
  await js(`(() => {
    const rate = document.querySelector('[role="tabpanel"][data-active="true"] input[inputmode="numeric"]');
    Object.getOwnPropertyDescriptor(HTMLInputElement.prototype,'value').set.call(rate,'57600');
    rate.dispatchEvent(new Event('input',{bubbles:true}));
    document.querySelector('.settings-sections [data-id="serial"]').focus();
  })()`)
  assert.equal(await js(`document.querySelectorAll('.settings-sections button svg').length`), 0)
  await js(`document.querySelector('[aria-label="选择波特率档位"]').click()`)
  await until(`!!document.querySelector('[data-slot="model-picker-panel"] [data-id="115200"]')`)
  await js(`document.querySelector('[data-slot="model-picker-panel"] [data-id="115200"]').click()`)
  await until(`document.querySelector('.baud-value input').value === '115200'`)
  await until(`!document.querySelector('[data-slot="model-picker-panel"]')`)
  await js(`document.querySelector('.baud-value input').focus()`)
  await key('Down')
  await until(`document.querySelector('.baud-value input').value === '57600'`)
  await js(`document.querySelector('.settings-sections [data-id="serial"]').focus()`)
  await key('Down')
  await until(`document.querySelector('[role="tab"][aria-selected="true"]').dataset.id === 'model'`)
  assert.equal(await js(`document.querySelectorAll('[role="tabpanel"]:not([inert])').length`), 1)
  await key('Down')
  await until(`document.querySelector('[role="tab"][aria-selected="true"]').dataset.id === 'identity'`)
  await key('Home')
  await until(`document.querySelector('[role="tabpanel"][data-active="true"] input[inputmode="numeric"]')?.value === '57600'`)
  await js(`(async () => {
    const snapshot = await window.serial.bootstrap();
    snapshot.configuredPorts = structuredClone(snapshot.configuredPorts);
    snapshot.transportProfiles = structuredClone(snapshot.transportProfiles);
    await window.serial.savePreferences({...snapshot.preferences});
  })()`)
  await js(`document.querySelector('.settings-sections [data-id="application"]').click()`)
  await until(`document.querySelector('.app-settings .more-trigger')?.getAttribute('aria-expanded') === 'false'`)
  assert.deepEqual(await js(`[...document.querySelectorAll('.app-settings legend')].map(el => el.textContent)`), ['外观', '字号', '后端连接'])
  await js(`document.querySelector('.app-settings .more-trigger').click()`)
  await until(`!!document.querySelector('.app-settings [aria-label="自动启动本地后端"]')`)
  await js(`document.querySelector('.app-settings .more-trigger').click()`)
  await until(`!!document.querySelector('[data-serial-number="终端字号（px）"] button')`)
  await js(`document.querySelector('[data-serial-number="终端字号（px）"] button').click()`)
  await until(`!!document.querySelector('[role="option"][data-id="18"]')`)
  await js(`document.querySelector('[role="option"][data-id="18"]').click()`)
  await until(`document.documentElement.style.getPropertyValue('--terminal-font-size') === '18px'`)
  assert.equal(await js(`(async () => (await window.serial.bootstrap()).preferences.terminalFontSize)()`), 18)
  await until(`!document.querySelector('[data-slot="model-picker-panel"]')`)

  const baseNavFont = await js(`parseFloat(getComputedStyle(document.querySelector('.window-bar button')).fontSize)`)
  await js(`document.querySelector('[data-serial-number="界面字号（px）"] button').click()`)
  await until(`!!document.querySelector('[role="option"][data-id="22"]')`)
  await js(`document.querySelector('[role="option"][data-id="22"]').click()`)
  await until(`getComputedStyle(document.documentElement).fontSize === '22px'`)
  assert.ok(await js(`parseFloat(getComputedStyle(document.querySelector('.window-bar button')).fontSize) > ${baseNavFont} * 1.3`))
  assert.equal(await js(`document.documentElement.style.getPropertyValue('--terminal-font-size')`), '18px')
  assert.equal(await js(`(async () => (await window.serial.bootstrap()).preferences.interfaceFontSize)()`), 22)
  await js(`window.serial.savePreferences({interfaceFontSize:16})`)
  await until(`getComputedStyle(document.documentElement).fontSize === '16px'`)
  await js(`document.querySelector('[role="radio"][aria-label="浅色"]').click()`)
  await until(`document.documentElement.dataset.theme === 'light' && document.querySelector('[aria-label="浅色"]').getAttribute('aria-checked') === 'true'`)
  assert.equal(await js(`getComputedStyle(document.documentElement).fontSize`), '16px')
  assert.equal(await js(`document.querySelector('.baud-value input').value`), '57600')
  await screenshot('settings-components-light.png')
  await checkReleasePreferences()
  await js(`document.querySelector('[aria-label="浅色"]').focus()`)
  await key('End')
  await until(`document.documentElement.dataset.theme === 'dark'`)
  await js(`window.qaMacroDefinitions = {}; window.qaMacroSaves = []; window.qaMacroRuns = [];
    window.serial.listMacros = async query => { const all = Object.values(window.qaMacroDefinitions).filter(item => query.include_drafts || item.shared); return {catalog_revision:window.qaMacroSaves.length, macros:all, total:all.length, definition:window.qaMacroDefinitions[query.id]}; };
    window.serial.saveMacro = async request => { window.qaMacroSaves.push(request); const definition = {...request, language_version:1, revision:(request.expected_revision || 0)+1, updated_at_ns:0}; window.qaMacroDefinitions[request.id] = definition; return {catalog_revision:window.qaMacroSaves.length, definition}; };
    window.serial.runMacro = async (port, spec) => { window.qaMacroRuns.push({port,spec}); return {id:'qa-execution', port, description:'mock only', status:'succeeded', line:1, writes:1, outcome_uncertain:false}; }; true`)
  await js(`Array.from(document.querySelectorAll('[data-slot="gooey-nav"] button')).find(button => button.textContent.includes('宏')).click()`)
  await until(`!!document.querySelector('.macro-page')`)
  await js(`[...document.querySelectorAll('.macro-tabs button')].find(b => b.textContent === "指南").click()`)
  await until(`document.querySelectorAll('.guide-trigger[aria-expanded="false"]').length === 4`)
  await js(`document.querySelectorAll('.guide-trigger')[2].click()`)
  await until(`document.querySelector('.guide-content code')?.textContent === 'cmd("version")'`)
  await js(`[...document.querySelectorAll('.macro-tabs button')].find(b => b.textContent === "编辑").click()`)
  await js(`Array.from(document.querySelectorAll('button')).find(button => button.textContent === 'U-Boot 模板').click()`)
  await until(`document.querySelector('[aria-label="宏 ID"]').value === 'enter_uboot'`)
  await screenshot('macro-editor-dark.png')
  await js(`(async()=>{const {preferences}=await window.serial.bootstrap();await window.serial.savePreferences({...preferences,theme:'light'})})()`)
  await screenshot('macro-editor-light.png')
  await js(`document.querySelector('.macro-editor-panel button[type="submit"]').click()`)
  await until(`!!document.querySelector('.macro-notice')`)
  assert.equal(await js(`window.qaMacroSaves[0].shared`), false)
  assert.equal(await js(`document.querySelectorAll('.macro-list button').length`), 0)
  await js(`document.querySelector('[aria-label="筛选宏"]').click()`)
  await js(`document.querySelector('.macro-drafts-toggle input').click()`)
  await until(`document.querySelectorAll('.macro-list button').length === 1`)
  await js(`document.querySelector('.macro-run-panel [aria-expanded="false"]')?.click()`)
  await until(`!!document.querySelector('.macro-run-actions button')`)
  await js(`document.querySelector('.macro-run-actions button').click()`)
  await until(`window.qaMacroRuns.length === 1`)
  assert.deepEqual(await js(`window.qaMacroRuns[0].spec`), {macro_id:'enter_uboot', revision:1, args:{interval_ms:50}, timeout_seconds:30})
  await js(`[...document.querySelectorAll('.macro-tabs button')].find(b => b.textContent === "编辑").click()`)
  await js(`(() => { const input = document.querySelector('.macro-script'); Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, 'value').set.call(input, 'cmd("version");'); input.dispatchEvent(new Event('input', {bubbles:true})); })()`)
  await js(`document.querySelector('.macro-run-panel [aria-expanded="false"]')?.click()`)
  await until(`document.querySelector('.macro-run-actions button').disabled`)
  await js(`[...document.querySelectorAll('.macro-tabs button')].find(b => b.textContent === "编辑").click()`)
  await js(`document.querySelector('.macro-editor-panel button[type="submit"]').click()`)
  await until(`window.qaMacroSaves.length === 2`)
  assert.equal(await js(`window.qaMacroSaves[1].expected_revision`), 1)
  await js(`document.querySelector('.macro-run-panel [aria-expanded="false"]')?.click()`)
  await js(`document.querySelector('.macro-scroll').scrollTop = 1000`)
  await screenshot('macro-run-result-light.png')
  await until(`!!document.querySelector('.macro-execution [data-slot="step-player"]')`)
  // Feed recorded external events through the QA bridge from this test process.
  await window.loadFile(join(__dirname, '../out/renderer/index.html'), { query: { qa:'1' } })
  await until(`!!document.querySelector('.device-choice')`)
  const { previewRunEvents } = await import('../src/renderer/preview-run.ts')
  const recording = previewRunEvents(await js('window.serial.bootstrap()'))
  await js(`(async () => {
    const state = await window.serial.bootstrap()
    const port = state.configuredPorts.find(p => p.config.port === 'COM7')
    const run = port.active_run, lease = port.control, events = ${JSON.stringify(recording)}
    const observer = { id:'qa:observer', kind:'human', label:'值班工作台' }
    const now = Date.now()*1e6
    port.active_run = null
    port.control = { ...lease, owner: observer }
    port.pending_run_start = { id:'qa-request',port:'COM7',requester:run.owner,required_approver:observer,
      label:run.label,metadata:{},control_ttl_ms:30000,daemon_epoch:port.daemon_epoch,generation:port.generation,
      requested_wall_time_ns:now,expires_wall_time_ns:now+30e9,expected_control_id:lease.id,expected_fence:lease.fence }
    state.events.COM7 = events.slice(0,1)
    await window.serial.savePreferences({})
    document.querySelector('[data-device-port="COM7"] .device-choice').click()
    window.deliverRun = () => {
      let i=1
      const advance=async () => {
        const s = await window.serial.bootstrap()
        const e = events[i++], active = e.kind === 'run_started'
        if (active || e.kind === 'run_ended') s.configuredPorts = s.configuredPorts.map(p=>p.config.port==='COM7'
          ? {...p, active_run:active?run:null,control:active?lease:null,pending_run_start:null} : p)
        s.events = {...s.events,COM7:[...s.events.COM7,e]}
        await window.serial.savePreferences({})
        if (i<events.length) setTimeout(advance,active?1450:540)
      }
      setTimeout(advance,500)
    }
  })()`)
  await until(`document.querySelector('.history-toggle')?.textContent.trim()==='等待接管授权'`)
  const trace = await js(`(async () => {
    window.deliverRun()
    const labels = [], owners = {}, seenCommands = new Set()
    let previousY = 0, previousScreenY, reversed, folded = false, samples = 0, finishedAt
    const start = performance.now()
    await new Promise((resolve, reject) => {
      function sample(now) {
        const status = document.querySelector('.history-toggle').textContent.trim()
        const orb = document.querySelector('[data-slot="matrix-orb"]')
        if (labels.at(-1) !== status) labels.push(status)
        owners[status] = orb.dataset.state
        const nav = document.querySelector('[data-slot="rail-toc"][data-active-node]')
        const pointer = nav?.querySelector('[data-slot="rail-toc-plane"]')
        if (pointer) {
          const matrix = new DOMMatrix(getComputedStyle(pointer).transform)
          const rect = pointer.getBoundingClientRect()
          const screenY = rect.top + rect.height / 2
          if (!reversed && (matrix.d > 0.05 || matrix.f < previousY - 0.5 || screenY < previousScreenY - 1.5)) {
            reversed = { step: nav.dataset.activeNode, previousY, localY:matrix.f, previousScreenY, screenY, direction:matrix.d }
          }
          previousY = matrix.f
          previousScreenY = screenY
          samples++
          folded ||= !!nav.querySelector('.action-trigger[aria-expanded="false"]')
          nav.querySelectorAll('.command-step:not([inert] *)').forEach(row => seenCommands.add(row.textContent))
          if (nav.dataset.activeNode.startsWith('end:')) finishedAt ??= now
        }
        if (finishedAt && now - finishedAt > 500) resolve()
        else if (now - start > 25000) reject(Error('Timed out replaying Agent events'))
        else requestAnimationFrame(sample)
      }
      requestAnimationFrame(sample)
    })
    return { labels, owners, commands: [...seenCommands], reversed, folded, samples }
  })()`)
  assert.deepEqual(trace.labels, ['等待接管授权', 'Agent 正在读取传感器状态', 'Agent 正在读取固件版本', 'Agent 正在读取采样总线状态',
    'Agent 正在读取传感器标识', 'Agent 正在采样温湿度', '采样温湿度已完成', 'Agent 正在确认传感器状态', '读取传感器状态已完成'])
  assert.equal(trace.owners['Agent 正在读取传感器状态'], 'thinking')
  assert.equal(trace.owners['读取传感器状态已完成'], 'idle')
  assert.deepEqual(trace.commands, ['version', 'bus status', 'sensor id', 'sensor read'])
  assert.equal(await js("document.querySelector('.macro-summary strong').textContent"), '采样温湿度')
  assert.ok(trace.samples > 20)
  assert.equal(trace.reversed, undefined, JSON.stringify(trace.reversed))
  assert.equal(trace.folded, false)
  await until(`!!document.querySelector('.run-outline .rail-node[data-tone="done"][data-reached="true"]')`)
  assert.equal(await js(`document.querySelector('.history-toggle').getAttribute('aria-expanded')`), 'true')
  assert.equal(await js(`(() => {
    const end = document.querySelector('[data-rail-node^="end:"]').getBoundingClientRect()
    const view = document.querySelector('.agent-scroll').getBoundingClientRect()
    return end.top >= view.top - 1 && end.bottom <= view.bottom + 1
  })()`), true)
  await js(`document.querySelector('.action-trigger').click()`)
  await js(`[...document.querySelectorAll('.window-bar button')].find(b => b.textContent === '配置').click()`)
  await until(`!!document.querySelector('.settings-page')`)
  await js(`[...document.querySelectorAll('.window-bar button')].find(b => b.textContent === '控制台').click()`)
  await until(`document.querySelectorAll('.action-trigger').length === 3`)
  assert.deepEqual(await js(`[...document.querySelectorAll('.action-trigger')].map(b => b.getAttribute('aria-expanded'))`), ['false', 'true', 'true'])
  // Remount after the live buffer has moved past the Run. The same open
  // branches and named result must also work with events read from disk.
  await js(`(async () => {
    const state = await window.serial.bootstrap(), events = state.events.COM7
    const end = events.find(e => e.kind === 'run_ended')
    const record = { port: 'COM7', epoch: end.daemon_epoch, run: end.metadata.run,
      started_wall_time_ns: events[1].wall_time_ns, through_seq: end.seq }
    const original = window.serial.agentHistory
    window.serial.agentHistory = async port => ({ ...await original(port), runs: port === 'COM7' ? [record] : [] })
    window.archiveReads = 0
    window.serial.agentRunEvents = async () => { window.archiveReads++; return { events: events.filter(e => e.run_id), limited: false } }
    state.events.COM7 = [{ ...end, kind: 'rx', direction: 'rx', seq: end.seq + 1, run_id: null, text: 'sensor> ', metadata: {} }]
    await window.serial.savePreferences(state.preferences)
  })()`)
  await js(`[...document.querySelectorAll('.window-bar button')].find(b => b.textContent === '宏').click()`)
  await until(`!!document.querySelector('.macro-page')`)
  await js(`[...document.querySelectorAll('.window-bar button')].find(b => b.textContent === '控制台').click()`)
  await until(`document.querySelector('.history-toggle').textContent === '读取传感器状态已完成' && document.querySelectorAll('.action-trigger').length === 3`)
  assert.equal(await js('window.archiveReads'), 1)
  assert.deepEqual(await js(`[...document.querySelectorAll('.action-trigger')].map(b => b.getAttribute('aria-expanded'))`), ['false', 'true', 'true'])
  assert.equal(await js("document.querySelector('.macro-summary strong').textContent"), '采样温湿度')
  // Use a full scrollback so a lost follow intent cannot hide behind a short document.
  const firstLine = await js(`window.appendCom7 = async count => {
    const state = await window.serial.bootstrap(), rows = state.events.COM7, last = rows.at(-1)
    state.events.COM7 = [...rows, ...Array.from({length: count}, (_, i) => ({...last, seq: last.seq+i+1, text: 'incoming '+(last.seq+i+1)+'\\n'}))]
    await window.serial.savePreferences({})
    return state.events.COM7.at(-1).text.trim()
  }; window.tailGap = () => {
    const view = document.querySelector('[data-port="COM7"] .terminal-scroll')
    return view.scrollHeight - view.clientHeight - view.scrollTop
  }; window.appendCom7(100)`)
  await until(`document.querySelector('.terminal-output').textContent.includes(${JSON.stringify(firstLine)}) && window.tailGap() < 3`)
  await js(`document.querySelector('.terminal-scroll').dispatchEvent(new WheelEvent('wheel', {bubbles:true, deltaY:90}))`)
  const nextLine = await js('window.appendCom7(12)')
  await until(`document.querySelector('.terminal-output').textContent.includes(${JSON.stringify(nextLine)}) && window.tailGap() < 3`)
  await js(`{const view=document.querySelector('.terminal-scroll');view.scrollTop-=180}`)
  await until(`!!document.querySelector('.return-live')`)
  await js('window.appendCom7(12)')
  assert.ok(await js('window.tailGap() > 150'))
  await js(`{const view=document.querySelector('.terminal-scroll');view.scrollTop=view.scrollHeight}`)
  await until(`!document.querySelector('.return-live') && window.tailGap() < 3`)
  await js(`document.querySelector('.history-collapse').click()`)
  await until(`document.querySelector('.history-drawer').getBoundingClientRect().height < 1 && window.tailGap() < 3`)
  const initialHeight = await js(`document.querySelector('.terminal-scroll').clientHeight`)
  await js(`document.querySelector('.history-toggle').click()`)
  await js('window.appendCom7(12)')
  await until(`document.querySelector('.history-drawer').getBoundingClientRect().height > 140 && window.tailGap() < 3`)
  assert.ok(await js(`document.querySelector('.terminal-scroll').clientHeight < ${initialHeight} - 130`))
  await until(`(() => {
    const pane = document.querySelector('[data-port="COM7"]')
    const rows = pane.querySelectorAll('.terminal-output [data-seq]'), rect = rows[rows.length-1]?.getBoundingClientRect()
    const dock = pane.querySelector('.command-history-dock').getBoundingClientRect()
    return rect?.height > 0 && rect.bottom <= dock.top + 1 && rect.top >= pane.querySelector('.terminal-scroll').getBoundingClientRect().top
  })()`)
  await js(`document.querySelector('.terminal-scroll').scrollTop -= 400`)
  await until(`!!document.querySelector('.return-live')`)
  const readingGap = await js('window.tailGap()')
  await js(`document.querySelector('.history-collapse').click()`)
  await until(`document.querySelector('.history-drawer').getBoundingClientRect().height < 1 && Math.abs(window.tailGap() - ${readingGap}) <= 2`)
  await js(`document.querySelector('.history-toggle').click()`)
  await until(`document.querySelector('.history-drawer').getBoundingClientRect().height > 140 && Math.abs(window.tailGap() - ${readingGap}) <= 2`)
  assert.ok(await js(`!!document.querySelector('.return-live')`))
  await fill('')
  await key('Return')
  await until(`!document.querySelector('.return-live') && window.tailGap() < 3`)
  const delayedLine = await js('window.appendCom7(8)')
  await until(`document.querySelector('.terminal-output').textContent.includes(${JSON.stringify(delayedLine)}) && window.tailGap() < 3`)
  await js(`(async () => {
    const s = await window.serial.bootstrap(), base = s.events.COM6[0]
    s.events.COM6 = Array.from({length:12000}, (_,i)=>({...base,kind:'rx',direction:'rx',seq:i+1,text:(i===42?'earlier marker':'line '+i)+'\\n'}))
    await window.serial.savePreferences({})
    document.querySelector('[data-device-port="COM6"] .device-choice').click()
  })()`)
  await until(`Array.from(document.querySelectorAll('.terminal-output [data-seq]')).at(-1)?.dataset.seq === '12000'`)
  assert.ok(await js(`document.querySelectorAll('.terminal-output [data-seq], .terminal-output [data-first-seq]').length < 420`))
  await js(`(async () => {
    const s=await window.serial.bootstrap(), lines=s.events.COM6
    s.events.COM6=[...lines.slice(32),...lines.slice(-32).map(e=>({...e,seq:e.seq+32}))]
    await window.serial.savePreferences({})
  })()`)
  await until(`Array.from(document.querySelectorAll('.terminal-output [data-seq]')).at(-1)?.dataset.seq === '12032'`)
  await js(`{ const view=document.querySelector('.terminal-scroll'); view.focus(); view.scrollTop = view.scrollHeight / 2 }`)
  await until(`!!document.querySelector('.return-live')`)
  await js(`document.querySelector('.terminal-find-trigger').click()`)
  await js(`(() => {
    const input=document.querySelector('input[aria-label="查找"]')
    Object.getOwnPropertyDescriptor(HTMLInputElement.prototype,'value').set.call(input,'earlier marker')
    input.dispatchEvent(new Event('input',{bubbles:true}))
  })()`)
  await until(`document.querySelector('.term-search-hit.is-current')?.closest('[data-seq]')?.dataset.seq === '43'`)
  await js(`document.querySelector('[aria-label="关闭查找 (Escape)"]').click(); document.querySelector('.return-live').click()`)
  await until(`Array.from(document.querySelectorAll('.terminal-output [data-seq]')).at(-1)?.dataset.seq === '12032'`)
  await js(`(async () => {
    const s = await window.serial.bootstrap(), base = s.events.COM6[0]
    s.events.COM6 = Array.from({length:12000}, (_,i) => ({...base,seq:i+1,text:i<4000 ? 'boot '+i+'\\n' : (i%80===79 ? '#\\n' : '#')}))
    await window.serial.savePreferences({})
  })()`)
  await until(`document.querySelector('.terminal-output')?.textContent.startsWith('boot 0')`)
  assert.ok(await js(`(() => {
    const pre=document.querySelector('.terminal-output'), view=pre.parentElement, plain=pre.cloneNode(false)
    plain.textContent=pre.textContent
    Object.assign(plain.style,{position:'absolute',visibility:'hidden',width:pre.offsetWidth+'px'})
    view.append(plain)
    const sameHeight=Math.abs(plain.getBoundingClientRect().height-pre.getBoundingClientRect().height)<1
    plain.remove()
    return sameHeight
  })()`), 'Archived text must wrap to the exact height of the unsegmented terminal')
  assert.ok(await js(`document.querySelector('.terminal-scroll').scrollHeight / document.querySelector('.terminal-scroll').clientHeight > 100`))
  await js(`{ const v=document.querySelector('.terminal-scroll'); v.scrollTop=v.scrollHeight/2 }`)
  await until(`!!document.querySelector('.return-live')`)
  assert.ok(await js(`Math.abs(document.querySelector('.terminal-scroll').scrollTop/document.querySelector('.terminal-scroll').scrollHeight-.5)<.01`))
  await js(`window.visibleFlashRow=()=>{const view=document.querySelector('.terminal-scroll'), box=view.getBoundingClientRect();return document.caretRangeFromPoint(box.left+view.clientWidth/2,box.top+4)?.startContainer.parentElement.closest('[data-seq]')?.dataset.seq}; true`)
  await until(`!!window.visibleFlashRow()`)
  const pausedRow = await js('window.visibleFlashRow()')
  await js(`(async () => {
    const state=await window.serial.bootstrap(), rows=state.events.COM6, last=rows.at(-1)
    state.events.COM6=[...rows.slice(64),...Array.from({length:64},(_,i)=>({...last,seq:last.seq+i+1,text:'#'}))]
    await window.serial.savePreferences({})
  })()`)
  await until(`document.querySelector('.terminal-output').textContent.startsWith('boot 64\\n')`)
  assert.equal(await js('window.visibleFlashRow()'), pausedRow)
  await js(`document.querySelector('.terminal-scroll').scrollTop=0`)
  await until(`document.querySelector('.terminal-output [data-seq="65"]')?.textContent === 'boot 64\\n'`)
  await js(`document.querySelector('.diagnostic-trigger').click()`)
  await until(`!!document.querySelector('.input-diagnostics')`)
  await fill('')
  await key('Return')
  await until(`document.querySelector('.input-diagnostics tbody')?.textContent.includes('空回车')`)
  await js(`document.querySelector('.input-diagnostics header button').click()`)
  await fill('version')
  await key('Return')
  assert.equal(await js(`document.querySelectorAll('.input-diagnostics tbody tr').length`), 1)
  await js(`document.querySelector('.input-diagnostics header button').click()`)
  await fill('version')
  await key('Return')
  await until(`document.querySelector('.input-diagnostics tbody tr th')?.textContent.includes('命令')`)
  await until(`document.querySelector('.command-editor input').value === ''`)
  for (const char of 'status') window.webContents.sendInputEvent({ type: 'char', keyCode: char })
  await until(`document.querySelector('.command-editor input').value === 'status'`)
  await key('Return')
  await until(`document.querySelector('.command-editor input').value === '' && document.querySelector('.input-diagnostics').textContent.includes('文字输入至下一帧')`)
  await js(`document.querySelector('.diagnostic-trigger').click()`)
  await until(`(() => {const v=document.querySelector('.terminal-scroll');return v.scrollHeight-v.clientHeight-v.scrollTop<3})()`)
  await js(`(async () => {
    const state=await window.serial.bootstrap(), base=state.events.COM6[0]
    const lines=['ready\\r','\\nErasing at 0x00 -- 0% complete.']
    for(let n=1;n<=100;n++) lines.push('\\r', 'Erasing at 0x'+n.toString(16)+' -- '+n+'% complete.')
    lines.push('\\r\\ndone\\r\\n')
    state.events.COM6=lines.map((text,i)=>({...base,seq:i+1,text}))
    await window.serial.savePreferences({})
  })()`)
  await until(`document.querySelector('.terminal-output').textContent === 'ready\\nErasing at 0x64 -- 100% complete.\\ndone\\n'`)
  await checkTcpConfiguration()
  await checkNativeTitleBar()
  process.stdout.write('PASS: TCP configuration, model selection, in-app updates, native window controls, title-bar drag region, application preferences, docking, terminal following and external Agent history\n')
  app.exit(0)
}).catch((error) => { process.stderr.write(`${error.stack}\n`); app.exit(1) })
