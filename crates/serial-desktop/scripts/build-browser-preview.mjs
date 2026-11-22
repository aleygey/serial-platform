// Export the Electron renderer with the browser sample bridge.
import { copyFile, mkdir, readFile, writeFile } from 'node:fs/promises'
import { dirname, extname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { parseArgs } from 'node:util'

const project = fileURLToPath(new URL('../', import.meta.url))
const renderer = resolve(project, 'out/renderer')
const { values } = parseArgs({ options: { 'site-dir': { type: 'string' } } })
const hosted = Boolean(values['site-dir'])
const output = hosted ? resolve(values['site-dir']) : resolve(project, 'qa/browser-preview')
const template = (await readFile(resolve(renderer, 'index.html'), 'utf8'))
  .replace('<html ', '<html data-serial-preview ')
  .replace('<title>Serial Platform</title>', '<title>串口控制台 · 交互预览</title>')
const script = template.match(/<script\b[^>]*src="([^"]+)"[^>]*><\/script>/)
const style = template.match(/<link\b[^>]*rel="stylesheet"[^>]*href="([^"]+)"[^>]*>/)
if (!script || !style) throw new Error('Missing renderer bundle in out/renderer/index.html')
const cssFile = resolve(renderer, style[1])
let styles = await readFile(cssFile, 'utf8')
const assets = new Set([script[1], style[1]])
for (const match of new Set([...styles.matchAll(/url\(([^)]+)\)/g)].map(match => match[1]))) {
  const file = match.replace(/^['"]|['"]$/g, '')
  if (file.startsWith('data:') || file.startsWith('#')) continue
  const mime = { '.woff': 'font/woff', '.woff2': 'font/woff2' }[extname(file)]
  if (!mime) throw new Error('Unknown preview asset: ' + file)
  if (hosted) assets.add(dirname(style[1]) + '/' + file)
  else {
    const asset = await readFile(resolve(dirname(cssFile), file))
    styles = styles.replaceAll('url(' + match + ')', () => 'url(data:' + mime + ';base64,' + asset.toString('base64') + ')')
  }
}
const notices = await Promise.all([
  'RARE-UI-LICENSE.txt', 'MOTION-PRIMITIVES-LICENSE.txt', 'ANIMATA-LICENSE.txt',
  'node_modules/@fontsource-variable/inter/LICENSE', 'node_modules/@fontsource/jetbrains-mono/LICENSE',
].map(name => readFile(resolve(project, name), 'utf8')))
let html = template.replace('</html>', () => '<!--\n' + notices.join('\n').replaceAll('-->', '-- >') + '\n-->\n</html>')
await mkdir(output, { recursive: true })
if (hosted) {
  for (const asset of assets) {
    const destination = resolve(output, asset)
    await mkdir(dirname(destination), { recursive: true })
    await copyFile(resolve(renderer, asset), destination)
  }
} else {
  const scriptText = await readFile(resolve(renderer, script[1]), 'utf8')
  html = html.replace(style[0], () => '<style>' + styles + '</style>')
    .replace(script[0], () => '<script type="module">' + scriptText.replace(/<\/script/gi, '<\\/script') + '</script>')
}
await writeFile(resolve(output, 'index.html'), html)
console.log(resolve(output, 'index.html'))
