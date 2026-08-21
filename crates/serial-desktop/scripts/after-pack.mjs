import { readFile, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import plist from 'plist'

const unusedPermissionKeys = [
  'NSAudioCaptureUsageDescription',
  'NSBluetoothAlwaysUsageDescription',
  'NSBluetoothPeripheralUsageDescription',
  'NSCameraUsageDescription',
  'NSMicrophoneUsageDescription'
]

export default async function afterPack(context) {
  if (context.electronPlatformName !== 'darwin') return
  const appName = context.packager.appInfo.productFilename
  const infoPath = join(context.appOutDir, `${appName}.app`, 'Contents', 'Info.plist')
  const info = plist.parse(await readFile(infoPath, 'utf8'))
  for (const key of unusedPermissionKeys) delete info[key]
  await writeFile(infoPath, plist.build(info), 'utf8')
}
