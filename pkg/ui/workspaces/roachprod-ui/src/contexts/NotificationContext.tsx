// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React, { createContext, useContext, useState, useCallback } from 'react';
import { message, Button, Space } from 'antd';
import { CloseOutlined } from '@ant-design/icons';

export interface Notification {
  id: string;
  type: 'success' | 'error' | 'warning' | 'info';
  message: string;
  description?: string;
  timestamp: Date;
}

interface NotificationContextType {
  notifications: Notification[];
  addNotification: (type: Notification['type'], msg: string, description?: string) => void;
  removeNotification: (id: string) => void;
  clearAll: () => void;
  unreadCount: number;
}

const NotificationContext = createContext<NotificationContextType | undefined>(undefined);

export const NotificationProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [notifications, setNotifications] = useState<Notification[]>([]);

  const addNotification = useCallback((type: Notification['type'], msg: string, description?: string) => {
    const id = `${Date.now()}-${Math.random()}`;
    const notification: Notification = {
      id,
      type,
      message: msg,
      description,
      timestamp: new Date(),
    };

    // Add to history
    setNotifications(prev => [notification, ...prev].slice(0, 50)); // Keep last 50

    // Show toast notification
    if (type === 'error') {
      // Errors are sticky - require manual dismissal
      // Show both message and description if available with a close button
      const content = (
        <div style={{ display: 'flex', alignItems: 'flex-start', gap: 12, width: '100%' }}>
          <div style={{ flex: 1 }}>
            {description ? (
              <>
                <div style={{ fontWeight: 600 }}>{msg}</div>
                <div style={{ fontSize: 13, marginTop: 4, opacity: 0.85 }}>{description}</div>
              </>
            ) : (
              <div>{msg}</div>
            )}
          </div>
          <Button
            type="text"
            size="small"
            icon={<CloseOutlined />}
            onClick={() => message.destroy(id)}
            style={{ marginTop: -4 }}
          />
        </div>
      );

      message.error({
        content,
        duration: 0, // Don't auto-dismiss
        key: id,
      });
    } else if (type === 'success') {
      // Success messages auto-dismiss after 5s
      message.success({
        content: msg,
        duration: 5,
      });
    } else if (type === 'warning') {
      message.warning({
        content: msg,
        duration: 8,
      });
    } else {
      message.info({
        content: msg,
        duration: 5,
      });
    }
  }, []);

  const removeNotification = useCallback((id: string) => {
    setNotifications(prev => prev.filter(n => n.id !== id));
    // Also dismiss the toast if it's still showing
    message.destroy(id);
  }, []);

  const clearAll = useCallback(() => {
    setNotifications([]);
    message.destroy();
  }, []);

  const unreadCount = notifications.filter(n => n.type === 'error').length;

  return (
    <NotificationContext.Provider
      value={{
        notifications,
        addNotification,
        removeNotification,
        clearAll,
        unreadCount,
      }}
    >
      {children}
    </NotificationContext.Provider>
  );
};

export const useNotifications = () => {
  const context = useContext(NotificationContext);
  if (!context) {
    throw new Error('useNotifications must be used within NotificationProvider');
  }
  return context;
};
