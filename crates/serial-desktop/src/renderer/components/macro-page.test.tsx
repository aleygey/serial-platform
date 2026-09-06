import { renderToStaticMarkup } from 'react-dom/server'
import { describe, expect, it, vi } from 'vitest'
import { MacroPage, macroRunArguments, parseMacroParameters } from './MacroPage'

describe('human Macro editor', () => {
  it('requires explicit sharing and a saved revision before actual execution', () => {
    const markup = renderToStaticMarkup(<MacroPage connected ports={[]} modelFamilies={[]} drafts={{ drafts: new Map() }} onExecution={vi.fn()} />)
    expect(markup).toContain('共享到默认宏列表')
    expect(markup).not.toContain('checked=""')
    expect(markup).toContain('修改保存后才能试运行')
    expect(markup).toContain('保存仅校验语法，试运行会实际操作串口')
  })

  it('passes typed parameter values rather than interpolating them into source', () => {
    const parameters = parseMacroParameters('{"interval":{"type":"integer","default":50,"minimum":20,"maximum":1000},"path":{"type":"string"},"enabled":{"type":"boolean","default":false}}')
    expect(macroRunArguments(parameters, { path: '"; cmd("reboot"); //' })).toEqual({ interval: 50, path: '"; cmd("reboot"); //', enabled: false })
    expect(() => macroRunArguments(parameters, { interval: '19', path: '' })).toThrow('范围')
    expect(() => macroRunArguments(parameters, { interval: '5e2', path: '' })).toThrow('整数')
    expect(() => macroRunArguments(parameters, {})).toThrow('path')
    expect(() => parseMacroParameters('[]')).toThrow('JSON 对象')
  })
})
