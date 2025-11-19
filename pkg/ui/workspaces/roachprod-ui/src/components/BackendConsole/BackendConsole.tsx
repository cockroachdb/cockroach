// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React, { useState, useEffect, useRef } from 'react';
import { Button } from 'antd';
import { UpOutlined, DownOutlined, ClearOutlined } from '@ant-design/icons';
import styles from './BackendConsole.module.scss';

interface SpinnerState {
  [nodeNum: string]: 'copying' | 'done';
}

const BackendConsole: React.FC = () => {
  const [isExpanded, setIsExpanded] = useState(false);
  const [logs, setLogs] = useState<string[]>([]);
  const [spinnerState, setSpinnerState] = useState<SpinnerState | null>(null);
  const [spinnerFrame, setSpinnerFrame] = useState(0);
  const logsEndRef = useRef<HTMLDivElement>(null);
  const wsRef = useRef<WebSocket | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  // Spinner animation
  useEffect(() => {
    if (!spinnerState) return;

    const interval = setInterval(() => {
      setSpinnerFrame(prev => (prev + 1) % 4);
    }, 100); // Update every 100ms for smooth animation

    return () => clearInterval(interval);
  }, [spinnerState]);

  // Auto-scroll to bottom when new logs arrive
  useEffect(() => {
    if (isExpanded) {
      logsEndRef.current?.scrollIntoView({ behavior: 'smooth' });
    }
  }, [logs, isExpanded, spinnerState]);

  // Process ANSI escape codes to handle cursor movement and line clearing
  const processLogMessage = (message: string, currentLogs: string[]): string[] => {
    // Remove all ANSI escape codes commonly used for terminal control
    const ansiEscapeRegex = /\x1B\[[0-9;]*[ABCDEFGHJKSTfm]/g;
    let processedMessage = message.replace(ansiEscapeRegex, '');

    // If the processed message is empty or only whitespace, skip it
    if (processedMessage.trim() === '') {
      return currentLogs;
    }

    // Check if message starts with carriage return (indicates line overwrite)
    // This is common for spinners that update in place
    if (processedMessage.startsWith('\r')) {
      // Remove the \r and any leading whitespace
      processedMessage = processedMessage.substring(1);

      // If we have previous logs and the last one doesn't end with newline,
      // replace it (this handles spinner updates)
      if (currentLogs.length > 0) {
        const lastLog = currentLogs[currentLogs.length - 1];
        if (!lastLog.endsWith('\n')) {
          // Replace the last line
          return [...currentLogs.slice(0, -1), processedMessage];
        }
      }
    }

    // Check if this looks like a spinner line
    // Pattern 1: "action text [spinner-char]" - e.g., "Destroying VMs |" or "generating ssh key 0/1 |"
    // Pattern 2: "cluster-name: action text X/Y [spinner-char]" - e.g., "storm-test: executing sql 0/1 |"
    // Pattern 3: "cluster-name: action X/Y [spinner]" - e.g., "storm-test: executing sql 0/1 |"
    // Pattern 4: "cluster-name:[node list]: action (item) X/Y [spinner]" - e.g., "storm-test:[1 2]: staging binary (cockroach) 0/2 |"
    // Pattern 5: "node: copying [spinner]" or "node: done" - e.g., "1: copying -" or "2: done"
    const spinnerPattern1 = /^[a-zA-Z][\w\s_]+\s+[|/\-\\]\s*$/;  // Any text ending with spinner char
    const spinnerPattern2 = /^[a-zA-Z][\w\s_]+\s+\d+\/\d+\s+[|/\-\\]?\s*$/;  // Text with progress counter and optional spinner
    const spinnerPattern3 = /^[\w-]+:\s+.+\s+\d+\/\d+\s+[|/\-\\]?\s*$/;  // "cluster-name: action X/Y [spinner]"
    const spinnerPattern4 = /^[\w-]+:\[[\d\s]+\]:\s+.+\s+\d+\/\d+\s+[|/\-\\]?\s*$/;  // "cluster-name:[nodes]: staging action X/Y [spinner]"
    const spinnerPattern5 = /^\d+:\s+(copying\s+[|/\-\\]|done)\s*$/;  // "node: copying [spinner]" or "node: done"

    const isSpinner = spinnerPattern1.test(processedMessage.trim()) ||
                      spinnerPattern2.test(processedMessage.trim()) ||
                      spinnerPattern3.test(processedMessage.trim()) ||
                      spinnerPattern4.test(processedMessage.trim()) ||
                      spinnerPattern5.test(processedMessage.trim());

    if (isSpinner) {
      // Extract node number if this is a node-specific spinner (pattern 5)
      const nodeMatch = processedMessage.match(/^(\d+):/);
      const nodeNum = nodeMatch ? nodeMatch[1] : null;

      if (nodeNum) {
        // For node-specific spinners, replace the last spinner from the same node
        // Find the last log entry from this node
        for (let i = currentLogs.length - 1; i >= 0; i--) {
          const log = currentLogs[i];
          const logTrimmed = log.replace(/\n$/, '');
          const logNodeMatch = logTrimmed.match(/^(\d+):/);

          if (logNodeMatch && logNodeMatch[1] === nodeNum) {
            // Check if this old entry was also a spinner
            const oldIsSpinner = spinnerPattern5.test(logTrimmed);
            if (oldIsSpinner) {
              // Replace this specific node's previous spinner
              return [...currentLogs.slice(0, i), processedMessage, ...currentLogs.slice(i + 1)];
            }
            break; // Found the node's last message but it wasn't a spinner, so append
          }
        }
      } else {
        // For non-node-specific spinners, use old logic
        if (currentLogs.length > 0) {
          const lastLog = currentLogs[currentLogs.length - 1];
          const lastLogTrimmed = lastLog.replace(/\n$/, '');
          const lastIsSpinner = spinnerPattern1.test(lastLogTrimmed) ||
                                spinnerPattern2.test(lastLogTrimmed) ||
                                spinnerPattern3.test(lastLogTrimmed) ||
                                spinnerPattern4.test(lastLogTrimmed);
          if (lastIsSpinner) {
            return [...currentLogs.slice(0, -1), processedMessage];
          }
        }
      }
    }

    // Normal message - append
    return [...currentLogs, processedMessage];
  };

  // Connect to log WebSocket
  useEffect(() => {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const host = window.location.host;
    const wsUrl = `${protocol}//${host}/api/logs`;

    const ws = new WebSocket(wsUrl);
    wsRef.current = ws;

    ws.onopen = () => {
      console.log('Backend console WebSocket connected');
      setLogs(prev => [...prev, '=== Connected to backend log stream ===\n']);
    };

    ws.onmessage = (event) => {
      const message = event.data;

      // Check for spinner state updates
      if (message.startsWith('__SPINNER_STATE__')) {
        const stateData = message.substring('__SPINNER_STATE__'.length).trim();
        const newState: SpinnerState = {};

        // Parse state like "1=copying;2=done;3=copying;"
        stateData.split(';').forEach((pair: string) => {
          const [node, state] = pair.split('=');
          if (node && state) {
            newState[node] = state as 'copying' | 'done';
          }
        });

        // Check if all nodes are done
        const allDone = Object.keys(newState).every((node: string) => newState[node] === 'done');
        if (allDone) {
          // Add final state to logs and clear spinner
          setLogs(prev => [
            ...prev,
            ...Object.keys(newState).sort((a, b) => parseInt(a) - parseInt(b)).map(node =>
              `   ${node}: done\n`
            )
          ]);
          setSpinnerState(null);
        } else {
          setSpinnerState(newState);
        }
      } else {
        // Regular log message
        setLogs(prev => processLogMessage(message, prev));
      }
    };

    ws.onerror = (error) => {
      console.error('Backend console WebSocket error:', error);
    };

    ws.onclose = () => {
      console.log('Backend console WebSocket closed');
      setLogs(prev => [...prev, '=== Disconnected from backend log stream ===\n']);
    };

    return () => {
      ws.close();
    };
  }, []);

  const handleClear = () => {
    setLogs([]);
  };

  const handleToggle = () => {
    setIsExpanded(!isExpanded);
    // Clear inline styles after state update to let CSS classes take control
    // Use setTimeout to ensure this happens after React re-renders
    setTimeout(() => {
      if (containerRef.current) {
        containerRef.current.style.removeProperty('width');
        containerRef.current.style.removeProperty('height');
      }
    }, 0);
  };

  const renderSpinner = (state: SpinnerState, frame: number): string => {
    const spinnerChars = ['|', '/', '-', '\\'];
    const spinnerChar = spinnerChars[frame];

    // Sort nodes numerically
    const nodes = Object.keys(state).sort((a, b) => parseInt(a) - parseInt(b));

    return nodes.map(node => {
      const status = state[node];
      if (status === 'done') {
        return `   ${node}: done\n`;
      } else {
        return `   ${node}: copying ${spinnerChar}\n`;
      }
    }).join('');
  };

  return (
    <div ref={containerRef} className={`${styles.consoleContainer} ${isExpanded ? styles.expanded : styles.collapsed}`}>
      <div className={styles.consoleHeader}>
        <span className={styles.consoleTitle}>Backend Console</span>
        <div className={styles.consoleActions}>
          <Button
            size="small"
            icon={<ClearOutlined />}
            onClick={handleClear}
            type="text"
            title="Clear logs"
          />
          <Button
            size="small"
            icon={isExpanded ? <DownOutlined /> : <UpOutlined />}
            onClick={handleToggle}
            type="text"
            title={isExpanded ? "Collapse" : "Expand"}
          />
        </div>
      </div>
      {isExpanded && (
        <div className={styles.consoleContent}>
          {logs.length === 0 && !spinnerState ? (
            <div className={styles.emptyState}>No activity yet</div>
          ) : (
            <pre className={styles.logText}>
              {logs.join('')}
              {spinnerState && renderSpinner(spinnerState, spinnerFrame)}
            </pre>
          )}
          <div ref={logsEndRef} />
        </div>
      )}
    </div>
  );
};

export default BackendConsole;
