import { HUMAN_COMMAND_UNCERTAIN_MESSAGE, type HumanCommandSubmission } from '../../shared/contracts'

export type ToastValue = { kind: 'notice' | 'error'; message: string }
export type SetToast = (value: ToastValue | undefined) => void

export async function submitHumanCommand(
  action: () => Promise<HumanCommandSubmission>,
  setToast: SetToast
): Promise<HumanCommandSubmission> {
  try {
    const submission = await action()
    if (
      !submission
      || !['accepted', 'rejected', 'uncertain'].includes(submission.status)
      || (submission.status !== 'accepted' && typeof submission.message !== 'string')
    ) {
      throw new Error('App 未收到可识别的人工命令结果')
    }
    if (submission.status !== 'accepted') {
      setToast({ kind: 'error', message: submission.message })
    }
    return submission
  } catch {
    const submission: HumanCommandSubmission = {
      status: 'uncertain',
      message: HUMAN_COMMAND_UNCERTAIN_MESSAGE
    }
    setToast({ kind: 'error', message: submission.message })
    return submission
  }
}

export async function withToast(
  action: () => Promise<void>,
  setToast: SetToast,
  announce = true
): Promise<boolean> {
  try {
    await action()
    if (announce) setToast({ kind: 'notice', message: '配置已保存并生效' })
    return true
  } catch (error) {
    setToast({ kind: 'error', message: message(error) })
    return false
  }
}

export function message(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}
