// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React, { useEffect, useRef, useState } from 'react';
import { Terminal } from 'xterm';
import { FitAddon } from 'xterm-addon-fit';
import { WebLinksAddon } from 'xterm-addon-web-links';
import 'xterm/css/xterm.css';
import { ClusterInfo } from '../../types/cluster';
import { getSQLWebSocketURL } from '../../api/roachprodApi';
import { Button, Select, Space } from 'antd';
import { CloseOutlined } from '@ant-design/icons';
import styles from '../SSHTerminal/SSHTerminal.module.scss';

interface SQLConsoleProps {
  cluster: ClusterInfo;
  onClose: () => void;
}

const SQLConsole: React.FC<SQLConsoleProps> = ({ cluster, onClose }) => {
  const terminalRef = useRef<HTMLDivElement>(null);
  const [terminal, setTerminal] = useState<Terminal | null>(null);
  const [ws, setWs] = useState<WebSocket | null>(null);
  const [selectedNode, setSelectedNode] = useState<number>(1);
  const fitAddonRef = useRef<FitAddon | null>(null);
  const wsRef = useRef<WebSocket | null>(null);

  useEffect(() => {
    if (!terminalRef.current) return;

    // Create terminal
    const term = new Terminal({
      cursorBlink: true,
      fontSize: 14,
      fontFamily: 'Menlo, Monaco, "Courier New", monospace',
      theme: {
        background: '#1e1e1e',
        foreground: '#d4d4d4',
      },
    });

    const fitAddon = new FitAddon();
    const webLinksAddon = new WebLinksAddon();

    term.loadAddon(fitAddon);
    term.loadAddon(webLinksAddon);
    term.open(terminalRef.current);

    fitAddonRef.current = fitAddon;

    // Fit after a short delay to ensure the container is fully rendered
    setTimeout(() => {
      fitAddon.fit();
    }, 0);

    // Handle window resize
    const handleResize = () => {
      fitAddon.fit();
      // Send new dimensions to backend if WebSocket is connected
      if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN && term) {
        // Reduce cols by 1 to prevent character cutoff on the right edge
        // This accounts for rounding errors in fitAddon calculations
        const cols = term.cols - 1;
        const rows = term.rows;
        const resizeMsg = JSON.stringify({ type: 'resize', cols, rows });
        wsRef.current.send(resizeMsg);
      }
    };
    window.addEventListener('resize', handleResize);

    setTerminal(term);

    return () => {
      window.removeEventListener('resize', handleResize);
      term.dispose();
    };
  }, []);

  useEffect(() => {
    if (!terminal) return;

    // Connect to WebSocket for SQL console
    const wsURL = getSQLWebSocketURL(cluster.name, selectedNode);
    const websocket = new WebSocket(wsURL);
    wsRef.current = websocket;

    websocket.onopen = () => {
      terminal.clear();
      terminal.writeln(`\x1b[32mConnected to ${cluster.name} node ${selectedNode}\x1b[0m`);
      terminal.writeln(`\x1b[36mStarting SQL console...\x1b[0m\r\n`);

      // Fit the terminal to the container and send dimensions immediately
      if (fitAddonRef.current) {
        fitAddonRef.current.fit();

        // Send terminal dimensions to backend BEFORE starting SQL
        // Reduce cols by 1 to prevent character cutoff on the right edge
        const cols = terminal.cols - 1;
        const rows = terminal.rows;
        const resizeMsg = JSON.stringify({ type: 'resize', cols, rows });
        websocket.send(resizeMsg);

        // Small delay to ensure resize is processed before starting SQL
        setTimeout(() => {
          websocket.send('./cockroach sql --certs-dir=certs\r');
        }, 50);
      } else {
        // No fit addon - start SQL immediately
        websocket.send('./cockroach sql --certs-dir=certs\r');
      }
    };

    websocket.onmessage = async (event) => {
      console.log('SQL WebSocket received:', event.data);
      // Handle both string and Blob data
      if (typeof event.data === 'string') {
        terminal.write(event.data);
      } else if (event.data instanceof Blob) {
        const text = await event.data.text();
        terminal.write(text);
      }
      // Scroll to bottom to ensure the cursor is visible
      // Use setTimeout to ensure the content is rendered first
      setTimeout(() => {
        terminal.scrollToBottom();
      }, 10);
    };

    websocket.onerror = (error) => {
      terminal.writeln(`\r\n\x1b[31mWebSocket error\x1b[0m`);
      console.error('WebSocket error:', error);
    };

    websocket.onclose = () => {
      terminal.writeln(`\r\n\x1b[33mConnection closed\x1b[0m`);
    };

    // Handle terminal input
    const disposable = terminal.onData((data) => {
      if (websocket.readyState === WebSocket.OPEN) {
        websocket.send(data);
      }
    });

    setWs(websocket);

    return () => {
      disposable.dispose();
      websocket.close();
    };
  }, [terminal, cluster.name, selectedNode]);

  const handleNodeChange = (nodeNum: number) => {
    setSelectedNode(nodeNum);
  };

  return (
    <div className={styles.terminalContainer}>
      <div className={styles.terminalHeader} style={{ background: '#ffffff' }}>
        <Space>
          <span className={styles.headerTitle}>
            SQL Console: {cluster.name}
          </span>
          <Select
            value={selectedNode}
            onChange={handleNodeChange}
            style={{ width: 120 }}
            options={Array.from({ length: cluster.nodes }, (_, i) => ({
              value: i + 1,
              label: `Node ${i + 1}`,
            }))}
          />
        </Space>
        <Button
          type="text"
          icon={<CloseOutlined />}
          onClick={onClose}
          className={styles.closeButton}
          style={{ color: 'black' }}
        />
      </div>
      <div className={styles.terminalContent} ref={terminalRef} />
    </div>
  );
};

export default SQLConsole;
