import '@fontsource-variable/inter'
import '@fontsource/jetbrains-mono/400.css'
import React from 'react'
import ReactDOM from 'react-dom/client'
import { App } from './App'
import { installQaBridge } from './qa-bridge'
import './styles.css'

const bridgeReady = installQaBridge()

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    {bridgeReady ? <App /> : (
      <div className="startup-screen">
        <strong>桌面通信模块未加载</strong>
        <small>请重新启动 Serial Platform；如果问题持续，请重新安装应用。</small>
      </div>
    )}
  </React.StrictMode>
)
