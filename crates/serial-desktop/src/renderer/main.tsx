import '@fontsource-variable/inter'
import '@fontsource/jetbrains-mono/400.css'
import React from 'react'
import ReactDOM from 'react-dom/client'
import { MotionConfig } from 'motion/react'
import { App } from './App'
import { WindowTitleBar } from './components/WindowTitleBar'
import { installQaBridge } from './qa-bridge'
import './component-utilities.css'
import './styles.css'
import './console.css'

const bridgeReady = installQaBridge()

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    {bridgeReady ? <MotionConfig reducedMotion="user"><App /></MotionConfig> : (
      <div className="app-frame"><WindowTitleBar /><div className="startup-screen">
        <strong>桌面通信模块未加载</strong>
        <small>请重新启动 Serial Platform；如果问题持续，请重新安装应用。</small>
      </div></div>
    )}
  </React.StrictMode>
)
