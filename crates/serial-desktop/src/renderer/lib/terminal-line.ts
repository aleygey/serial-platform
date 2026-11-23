/** Projects device bytes already decoded as text onto the terminal's unfinished
 * line. Completed lines never change, including when CR and LF arrive apart. */
export class TerminalLine {
  private line = ''
  private cursor = 0
  private control: 'text' | 'escape' | 'intermediate' | 'csi' | 'string' | 'stringEscape' = 'text'
  private parameters = ''

  reset(): void {
    this.line = ''
    this.cursor = 0
    this.control = 'text'
    this.parameters = ''
  }

  retain(length: number): void {
    const removed = this.line.length - length
    if (removed > 0) {
      this.line = this.line.slice(removed)
      this.cursor = Math.max(0, this.cursor - removed)
    }
  }

  write(raw: string): { remove: number; text: string } {
    if (this.control === 'text' && this.cursor === this.line.length && !/[\x00-\x08\x0a-\x1f\x7f-\x9f]/.test(raw)) {
      this.line += raw
      this.cursor += raw.length
      return { remove: 0, text: raw }
    }
    const originalLength = this.line.length
    let unchanged = originalLength
    let finished = ''
    const markEdit = (offset: number) => { if (!finished) unchanged = Math.min(unchanged, offset) }
    const writeText = (text: string) => {
      markEdit(this.cursor)
      const gap = Math.max(0, this.cursor - this.line.length)
      this.line = this.line.slice(0, this.cursor) + ' '.repeat(gap) + text + this.line.slice(this.cursor + text.length)
      this.cursor += text.length
    }
    const input = raw
    for (let i = 0; i < input.length;) {
      const char = input[i++]
      if (this.control === 'string' || this.control === 'stringEscape') {
        if (char === '\x07' || char === '\x9c' || char === '\x18' || char === '\x1a' ||
            (this.control === 'stringEscape' && char === '\\')) this.control = 'text'
        else this.control = char === '\x1b' ? 'stringEscape' : 'string'
        continue
      }
      if (char === '\x1b') { this.control = 'escape'; continue }
      if (char === '\x18' || char === '\x1a') { this.control = 'text'; continue }
      if (char === '\x9b' || (this.control === 'escape' && char === '[')) {
        this.control = 'csi'
        this.parameters = ''
        continue
      }
      // OSC, DCS, SOS, PM and APC are not printable text. Consume their
      // payload up to BEL or ST, including when the terminator spans packets.
      if ('\x90\x98\x9d\x9e\x9f'.includes(char) || (this.control === 'escape' && 'P]X^_'.includes(char))) {
        this.control = 'string'
        continue
      }
      if (this.control === 'escape' || this.control === 'intermediate') {
        this.control = char >= ' ' && char <= '/' ? 'intermediate' : 'text'
        continue
      }
      if (this.control === 'csi') {
        if (char >= ' ' && char <= '?') {
          if (this.parameters.length < 64) this.parameters += char
          continue
        }
        if (char < '@' || char > '~') continue
        this.control = 'text'
        const amount = Math.min(16384, parseInt(this.parameters, 10) || 0)
        if (char === 'G') this.cursor = Math.max(0, amount - 1)
        else if (char === 'C') this.cursor = Math.min(this.line.length + 16384, this.cursor + (amount || 1))
        else if (char === 'D') this.cursor = Math.max(0, this.cursor - (amount || 1))
        else if (char === 'K' && amount <= 2) {
          if (amount === 0) { markEdit(this.cursor); this.line = this.line.slice(0, this.cursor) }
          else if (amount === 1) {
            markEdit(0)
            const end = Math.min(this.line.length, this.cursor + 1)
            this.line = ' '.repeat(end) + this.line.slice(end)
          } else { markEdit(0); this.line = '' }
        }
        continue
      }
      if (char === '\r') { this.cursor = 0; continue }
      if (char === '\n') {
        finished += this.line + '\n'
        this.line = ''
        this.cursor = 0
        continue
      }
      if (char === '\b' || char === '\x7f') {
        if (this.cursor > 0) {
          this.cursor--
          if (this.cursor > 0 && /[\uDC00-\uDFFF]/.test(this.line[this.cursor] ?? '')) this.cursor--
        }
        continue
      }
      if ((char < ' ' && char !== '\t') || (char >= '\x80' && char <= '\x9f')) continue
      const begin = i - 1
      while (i < input.length && ((input[i] >= ' ' && input[i] < '\x7f') || input[i] > '\x9f' || input[i] === '\t')) i++
      writeText(input.slice(begin, i))
    }
    return { remove: originalLength - unchanged, text: (finished + this.line).slice(unchanged) }
  }
}
