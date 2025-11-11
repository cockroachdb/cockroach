// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React, { useEffect, useRef, useState } from 'react';
import { Terminal } from 'xterm';
import { FitAddon } from 'xterm-addon-fit';
import { WebLinksAddon } from 'xterm-addon-web-links';
import { Select, Button, Space, theme } from 'antd';
import { CloseOutlined } from '@ant-design/icons';
import { ClusterInfo } from '../../types/cluster';
import { getSSHWebSocketURL } from '../../api/roachprodApi';
import styles from './SSHTerminal.module.scss';

const { Option } = Select;
const { useToken } = theme;

interface SSHTerminalProps {
  cluster: ClusterInfo;
  onClose: () => void;
}

const SSHTerminal: React.FC<SSHTerminalProps> = ({ cluster, onClose }) => {
  const { token } = useToken();
  const terminalRef = useRef<HTMLDivElement>(null);
  const terminal = useRef<Terminal | null>(null);
  const fitAddon = useRef<FitAddon | null>(null);
  const ws = useRef<WebSocket | null>(null);
  const intentionalClose = useRef<boolean>(false);
  const [selectedNode, setSelectedNode] = useState(1);
  const [connectionStatus, setConnectionStatus] = useState<'disconnected' | 'connecting' | 'connected'>('disconnected');

  // Always use dark terminal (terminals traditionally have dark backgrounds)
  const isDark = true;

  useEffect(() => {
    if (!terminalRef.current) return;

    // Create terminal with theme-aware colors
    terminal.current = new Terminal({
      cursorBlink: true,
      fontSize: 14,
      fontFamily: 'Menlo, Monaco, "Courier New", monospace',
      theme: isDark ? {
        // Dark theme (VS Code Dark+)
        background: '#1e1e1e',
        foreground: '#d4d4d4',
        cursor: '#ffffff',
        selectionBackground: '#264f78',
        black: '#000000',
        red: '#cd3131',
        green: '#0dbc79',
        yellow: '#e5e510',
        blue: '#2472c8',
        magenta: '#bc3fbc',
        cyan: '#11a8cd',
        white: '#e5e5e5',
        brightBlack: '#666666',
        brightRed: '#f14c4c',
        brightGreen: '#23d18b',
        brightYellow: '#f5f543',
        brightBlue: '#3b8eea',
        brightMagenta: '#d670d6',
        brightCyan: '#29b8db',
        brightWhite: '#ffffff',
      } : {
        // Light theme
        background: '#ffffff',
        foreground: '#383a42',
        cursor: '#383a42',
        selectionBackground: '#d7dae0',
        black: '#383a42',
        red: '#e45649',
        green: '#50a14f',
        yellow: '#c18401',
        blue: '#0184bc',
        magenta: '#a626a4',
        cyan: '#0997b3',
        white: '#fafafa',
        brightBlack: '#4f525e',
        brightRed: '#e06c75',
        brightGreen: '#98c379',
        brightYellow: '#e5c07b',
        brightBlue: '#61afef',
        brightMagenta: '#c678dd',
        brightCyan: '#56b6c2',
        brightWhite: '#ffffff',
      },
    });

    // Add addons
    fitAddon.current = new FitAddon();
    terminal.current.loadAddon(fitAddon.current);
    terminal.current.loadAddon(new WebLinksAddon());

    // Open terminal
    terminal.current.open(terminalRef.current);
    fitAddon.current.fit();

    // Handle resize with debouncing to avoid sending incorrect dimensions
    let resizeTimeout: number | null = null;
    const handleResize = () => {
      if (resizeTimeout) {
        clearTimeout(resizeTimeout);
      }

      resizeTimeout = setTimeout(() => {
        if (fitAddon.current && terminal.current && terminalRef.current) {
          // Use requestAnimationFrame to ensure layout is complete
          requestAnimationFrame(() => {
            if (fitAddon.current && terminal.current && terminalRef.current) {
              const containerWidthBefore = terminalRef.current.offsetWidth;
              const containerHeightBefore = terminalRef.current.offsetHeight;
              console.log(`SSH Terminal: Window resize - BEFORE fit() - container: ${containerWidthBefore}x${containerHeightBefore}`);

              // Only fit if container has reasonable dimensions (not collapsed)
              if (containerWidthBefore < 50) {
                console.log('SSH Terminal: Container too narrow, skipping resize');
                return;
              }

              fitAddon.current.fit();

              // Wait for fit to complete and send dimensions
              requestAnimationFrame(() => {
                if (ws.current && ws.current.readyState === WebSocket.OPEN && terminal.current && terminalRef.current) {
                  const containerWidth = terminalRef.current.offsetWidth;
                  const containerHeight = terminalRef.current.offsetHeight;
                  let cols = terminal.current.cols;
                  const rows = terminal.current.rows;

                  // Sanity check: don't send if dimensions are too small
                  if (cols < 10 || rows < 5) {
                    console.log(`SSH Terminal: Dimensions too small (cols=${cols}, rows=${rows}), skipping resize message`);
                    return;
                  }

                  // Reduce cols by 1 to prevent character cutoff on the right edge
                  // This accounts for rounding errors in fitAddon calculations
                  cols = cols - 1;

                  console.log(`SSH Terminal: Window resize - AFTER fit() - container: ${containerWidth}x${containerHeight}, terminal: cols=${cols}, rows=${rows}`);
                  const resizeMsg = JSON.stringify({ type: 'resize', cols, rows });
                  ws.current.send(resizeMsg);
                }
              });
            }
          });
        }
      }, 150);
    };
    window.addEventListener('resize', handleResize);

    // Also observe container size changes (handles browser resize, panel resize, etc.)
    let resizeObserver: ResizeObserver | null = null;
    if (terminalRef.current) {
      resizeObserver = new ResizeObserver(() => {
        handleResize();
      });
      resizeObserver.observe(terminalRef.current);
    }

    return () => {
      window.removeEventListener('resize', handleResize);
      if (resizeObserver && terminalRef.current) {
        resizeObserver.unobserve(terminalRef.current);
        resizeObserver.disconnect();
      }
      terminal.current?.dispose();
    };
  }, [isDark]); // Recreate terminal when theme changes

  useEffect(() => {
    if (!terminal.current) return;

    // Clear terminal
    terminal.current.clear();
    setConnectionStatus('connecting');

    // Create WebSocket connection
    const wsUrl = getSSHWebSocketURL(cluster.name, selectedNode);
    const socket = new WebSocket(wsUrl);
    socket.binaryType = 'arraybuffer'; // Expect binary data as ArrayBuffer

    socket.onopen = () => {
      // Only update status and write message if this is still the current socket
      if (ws.current === socket) {
        setConnectionStatus('connected');
        terminal.current?.writeln(`\r\n\x1b[32mConnected to ${cluster.name}:${selectedNode}\x1b[0m\r\n`);

        // Send initial terminal dimensions to backend
        if (terminal.current && fitAddon.current) {
          fitAddon.current.fit();

          // Small delay to ensure fit() has completed before reading dimensions
          setTimeout(() => {
            if (terminal.current && ws.current === socket && terminalRef.current) {
              const containerWidth = terminalRef.current.offsetWidth;
              const containerHeight = terminalRef.current.offsetHeight;
              const cols = terminal.current.cols;
              const rows = terminal.current.rows;
              console.log(`SSH Terminal: Initial resize - container: ${containerWidth}x${containerHeight}, terminal: cols=${cols}, rows=${rows}`);
              const resizeMsg = JSON.stringify({ type: 'resize', cols, rows });
              socket.send(resizeMsg);
            }
          }, 100);
        }
      }
    };

    socket.onmessage = async (event) => {
      // Only process messages from the current socket
      if (terminal.current && ws.current === socket) {
        // WebSocket binary messages come as Blob or ArrayBuffer
        if (event.data instanceof Blob) {
          const text = await event.data.text();
          terminal.current.write(text);
        } else if (event.data instanceof ArrayBuffer) {
          const text = new TextDecoder().decode(event.data);
          terminal.current.write(text);
        } else {
          // Text message
          terminal.current.write(event.data);
        }
      }
    };

    socket.onerror = (error) => {
      // Only show errors for the current socket
      if (ws.current === socket) {
        setConnectionStatus('disconnected');
        terminal.current?.writeln(`\r\n\x1b[31mConnection error\x1b[0m\r\n`);
      }
    };

    socket.onclose = () => {
      // Only show close message for the current socket
      if (ws.current === socket) {
        setConnectionStatus('disconnected');
        terminal.current?.writeln(`\r\n\x1b[33mConnection closed\x1b[0m\r\n`);
      }
    };

    // Close existing WebSocket if any
    if (ws.current) {
      ws.current.close();
    }

    ws.current = socket;

    // Handle terminal input
    const disposable = terminal.current.onData((data) => {
      if (socket.readyState === WebSocket.OPEN) {
        socket.send(data);
      }
    });

    return () => {
      disposable.dispose();
      // Don't close the socket here - it will be closed when we create the next one
      // or when the component unmounts (handled by onClose callback)
    };
  }, [cluster.name, selectedNode]);

  const nodeOptions = Array.from({ length: cluster.nodes }, (_, i) => i + 1);

  const getStatusColor = () => {
    switch (connectionStatus) {
      case 'connected':
        return '#52c41a';
      case 'connecting':
        return '#faad14';
      case 'disconnected':
        return '#f5222d';
    }
  };

  const getStatusText = () => {
    switch (connectionStatus) {
      case 'connected':
        return 'Connected';
      case 'connecting':
        return 'Connecting...';
      case 'disconnected':
        return 'Disconnected';
    }
  };

  return (
    <div className={styles.terminalContainer}>
      <div
        className={styles.terminalHeader}
        style={{
          background: '#ffffff',
          borderBottom: `1px solid ${token.colorBorder}`,
        }}
      >
        <Space>
          <span style={{ color: token.colorText, fontWeight: 600, fontSize: 14 }}>
            {cluster.name}
          </span>
          <Select
            value={selectedNode}
            onChange={setSelectedNode}
            style={{ width: 120 }}
            size="small"
            options={nodeOptions.map((node) => ({
              key: node,
              value: node,
              label: `Node ${node}`,
            }))}
          />
          <span style={{ color: getStatusColor(), fontSize: 12 }}>
            ● {getStatusText()}
          </span>
        </Space>
        <Button
          type="text"
          icon={<CloseOutlined />}
          onClick={onClose}
          style={{ color: 'black' }}
        />
      </div>
      <div className={styles.terminalContent} ref={terminalRef} />
    </div>
  );
};

export default SSHTerminal;
