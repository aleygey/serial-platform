/** Projects device bytes already decoded as text onto the terminal's unfinished
 * line. Completed lines never change, including when CR and LF arrive apart. */
export class TerminalLine {
  private line = ''
  private cursor = 0
  private escape = ''

  reset(): void {
    this.line = ''
    this.cursor = 0
    this.escape = ''
  }

  retain(length: number): void {
    const removed = this.line.length - length
    if (removed > 0) {
      this.line = this.line.slice(removed)
      this.cursor = Math.max(0, this.cursor - removed)
    }
  }

  write(raw: string): { remove: number; text: string } {
    if (!this.escape && this.cursor === this.line.length && !/[\x00-\x08\x0a-\x1f\x7f]/.test(raw)) {
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
    const input = this.escape + raw
    this.escape = ''
    for (let i = 0; i < input.length;) {
      const char = input[i++]
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
      if (char === '\x1b') {
        const begin = i - 1
        if (i === input.length) { this.escape = char; break }
        if (input[i++] !== '[') continue
        const params = i
        while (i < input.length && input.charCodeAt(i) >= 0x20 && input.charCodeAt(i) <= 0x3f) i++
        if (i === input.length) {
          if (i - begin <= 64) this.escape = input.slice(begin)
          break
        }
        const amount = Math.min(16384, parseInt(input.slice(params, i), 10) || 0)
        const op = input[i++]
        if (op === 'G') this.cursor = Math.max(0, amount - 1)
        else if (op === 'C') this.cursor = Math.min(this.line.length + 16384, this.cursor + (amount || 1))
        else if (op === 'D') this.cursor = Math.max(0, this.cursor - (amount || 1))
        else if (op === 'K' && amount <= 2) {
          if (amount === 0) { markEdit(this.cursor); this.line = this.line.slice(0, this.cursor) }
          else if (amount === 1) {
            markEdit(0)
            const end = Math.min(this.line.length, this.cursor + 1)
            this.line = ' '.repeat(end) + this.line.slice(end)
          } else { markEdit(0); this.line = '' }
        }
        continue
      }
      if (char < ' ' && char !== '\t') { writeText('�'); continue }
      const begin = i - 1
      while (i < input.length && (input[i] >= ' ' && input[i] !== '\x7f' || input[i] === '\t')) i++
      writeText(input.slice(begin, i))
    }
    return { remove: originalLength - unchanged, text: (finished + this.line).slice(unchanged) }
  }
}
