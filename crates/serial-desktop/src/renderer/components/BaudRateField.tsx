import { useId, useRef } from 'react'
import { AnimatedCounter } from './ui/animated-counter'
import { ModelPicker } from './ui/model-picker'

const rates = [300, 600, 1200, 2400, 4800, 9600, 19200, 38400, 57600, 115200, 230400, 460800, 500000, 576000, 921600, 1000000, 1500000, 2000000, 3000000, 4000000]
export function BaudRateField({ value, onChange }: { value: number; onChange: (rate: number) => void }) {
  return <SerialNumberField label="波特率" value={value} choices={rates} editable onChange={onChange} />
}

export function SerialNumberField({ label, value, choices, editable = false, onChange }: {
  label: string; value: number; choices: readonly number[]; editable?: boolean; onChange: (value: number) => void
}) {
  const id = useId()
  const field = useRef<HTMLDivElement>(null)
  const numbers = !value || choices.includes(value) ? choices : [...choices, value].sort((a, b) => a - b)
  const options = numbers.map(n => ({ id: String(n), name: String(n) }))
  const open = () => field.current?.querySelector<HTMLButtonElement>('button[aria-haspopup]')?.click()
  return <div className="form-field">
    <label htmlFor={id}>{label}</label>
    <div className="baud-control" data-serial-number={label} ref={field}>
      <div className="baud-value">
        <input id={id} role="spinbutton" aria-valuenow={value} aria-valuemin={editable ? 1 : choices[0]} aria-valuemax={editable ? undefined : choices.at(-1)}
          readOnly={!editable} inputMode={editable ? 'numeric' : 'none'} pattern="[0-9]*" value={value || ''} onClick={editable ? undefined : open} onChange={event => {
          if (/^\d{0,10}$/.test(event.target.value)) onChange(Number(event.target.value))
        }} onKeyDown={event => {
          if (!editable && (event.key === ' ' || event.key === 'Enter')) { event.preventDefault(); open(); return }
          if (event.key !== 'ArrowDown' && event.key !== 'ArrowUp') return
          event.preventDefault()
          const next = event.key === 'ArrowUp' ? choices.find(n => n > value) : choices.filter(n => n < value).at(-1)
          if (next !== undefined) onChange(next)
        }} />
        <AnimatedCounter aria-hidden value={value} separator="" duration={0.28} />
      </div>
      <ModelPicker compact models={options} value={String(value)} label={editable ? '选择' + label + '档位' : label} onChange={n => onChange(Number(n))} />
    </div>
  </div>
}
