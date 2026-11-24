const assert = require('node:assert/strict')

// Executed in the built renderer with the offline serial fixture.
async function measureOutput(fontSize) {
  const frame = () => new Promise(resolve => requestAnimationFrame(resolve))
  const pane = document.querySelector('[data-port="COM6"]')
  const view = pane.querySelector('.terminal-scroll')
  const pre = pane.querySelector('.terminal-output')
  const peer = document.querySelector('[data-port="COM7"] .terminal-output')
  const base = (await window.serial.bootstrap()).events.COM6.find(event => event.direction === 'rx')
  const message = 'Heartbeat 1234 / 1234\n'
  const event = (seq, text) => ({...base, seq, text, kind:'rx', direction:'rx', run_id:null, stream_offset_start:undefined, stream_offset_end:undefined})
  async function publish(rows) {
    const snapshot = await window.serial.bootstrap()
    snapshot.events.COM6 = rows
    await window.serial.savePreferences({terminalFontSize:fontSize})
    const last = rows.at(-1)
    for (let i=0; i<100 && pre.querySelector('[data-seq="'+last.seq+'"]')?.textContent !== last.text.replace(/^\r/, ''); i++) await frame()
    await frame()
    await frame()
    if (!pre.querySelector('[data-seq="'+last.seq+'"]')) throw Error('The serial fixture did not render event ' + last.seq)
  }
  function glyph(output) {
    const span = Array.from(output.querySelectorAll('[data-seq]')).at(-1)
    const node = document.createTreeWalker(span, NodeFilter.SHOW_TEXT).nextNode()
    const range = document.createRange()
    range.setStart(node, 0)
    range.setEnd(node, 1)
    const rect = range.getBoundingClientRect()
    return {x:rect.x, y:rect.y, height:rect.height}
  }
  await document.fonts.ready
  const rows = [event(1, message)]
  await publish(rows.slice())
  const widthBeforeOverflow = view.clientWidth
  while (rows.length < 385) rows.push(event(rows.length+1, message))
  await publish(rows.slice())
  const widthAfterOverflow = view.clientWidth
  view.scrollTop = view.scrollHeight
  await frame()
  // The pane entry spring deliberately moves the whole pane after docking.
  for (let i=0; i<120 && [...document.querySelectorAll('.port-console')].some(el => getComputedStyle(el).transform !== 'none'); i++) await frame()
  const peerBefore = glyph(peer)
  const lines = [], progress = [], positions = []
  for (let i=0; i<40; i++) {
    const overwrite = i>=24
    rows.push(event(rows.length+1, overwrite ? '\rProgress '+i+' / 99' : message))
    await publish(rows.slice())
    const rect = glyph(pre)
    const viewport = view.getBoundingClientRect()
    const visible = Math.min(rect.y + rect.height, viewport.bottom) - Math.max(rect.y, viewport.top)
    const lineHeight = parseFloat(getComputedStyle(pre).lineHeight)
    if (visible <= 0 || (viewport.height >= lineHeight && visible < rect.height - 1/devicePixelRatio)) {
      throw Error('Tail glyph clipped: ' + JSON.stringify({rect, viewport, lineHeight, visible, windowHeight:innerHeight}))
    }
    if (overwrite) progress.push(rect.y)
    else {
      lines.push(rect.y)
      if (positions.length < 6) positions.push({y:rect.y, top:view.scrollTop, height:view.scrollHeight, viewport:view.clientHeight})
    }
  }
  const spread = samples => (Math.max(...samples)-Math.min(...samples))*devicePixelRatio
  return {density:devicePixelRatio, rowHeight:parseFloat(getComputedStyle(pre).lineHeight), drift:spread(lines),
    progressDrift:spread(progress), peerBefore, peerAfter:glyph(peer), widthBeforeOverflow, widthAfterOverflow, positions}
}

exports.checkLiveOutput = async function(window) {
  const contents = window.webContents
  const nativeScale = await contents.executeJavaScript('devicePixelRatio')
  try {
    for (const [density, font] of [[1,13],[1.25,13],[1.5,17],[2,22]]) {
      contents.setZoomFactor(density/nativeScale)
      const result = await contents.executeJavaScript(`(${measureOutput.toString()})(${font})`)
      const detail = JSON.stringify(result)
      assert.ok(Math.abs(result.density-density)<0.001, detail)
      assert.ok(Math.abs(result.rowHeight*density-Math.round(font*1.72*density))<0.01, detail)
      assert.ok(result.drift<0.1 && result.progressDrift<0.1, detail)
      assert.deepEqual(result.peerBefore, result.peerAfter)
      assert.equal(result.widthBeforeOverflow, result.widthAfterOverflow)
      console.log(`PASS: ${density*100}% / ${font}px, live text drift ${result.drift.toFixed(3)} physical pixels`)
    }
  } finally {
    contents.setZoomFactor(1)
  }
}
