// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from 'react';
import { Drawer, Button, Empty, Badge, Typography, Space } from 'antd';
import {
  BellOutlined,
  CloseOutlined,
  CheckCircleOutlined,
  CloseCircleOutlined,
  WarningOutlined,
  InfoCircleOutlined,
  CopyOutlined,
  DeleteOutlined,
} from '@ant-design/icons';
import { useNotifications, Notification } from '../../contexts/NotificationContext';
import styles from './NotificationDrawer.module.scss';

const { Text, Title } = Typography;

interface NotificationDrawerProps {
  open: boolean;
  onClose: () => void;
}

const NotificationDrawer: React.FC<NotificationDrawerProps> = ({ open, onClose }) => {
  const { notifications, removeNotification, clearAll } = useNotifications();

  const getIcon = (type: Notification['type']) => {
    switch (type) {
      case 'success':
        return <CheckCircleOutlined style={{ color: '#52c41a', fontSize: 20 }} />;
      case 'error':
        return <CloseCircleOutlined style={{ color: '#ff4d4f', fontSize: 20 }} />;
      case 'warning':
        return <WarningOutlined style={{ color: '#faad14', fontSize: 20 }} />;
      case 'info':
        return <InfoCircleOutlined style={{ color: '#1890ff', fontSize: 20 }} />;
    }
  };

  const copyToClipboard = (notification: Notification) => {
    const text = `[${notification.type.toUpperCase()}] ${notification.message}${
      notification.description ? `\n${notification.description}` : ''
    }\nTime: ${notification.timestamp.toLocaleString()}`;

    navigator.clipboard.writeText(text);
  };

  const formatTime = (date: Date) => {
    const now = new Date();
    const diff = now.getTime() - date.getTime();
    const seconds = Math.floor(diff / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);
    const days = Math.floor(hours / 24);

    if (seconds < 60) return 'Just now';
    if (minutes < 60) return `${minutes}m ago`;
    if (hours < 24) return `${hours}h ago`;
    if (days < 7) return `${days}d ago`;
    return date.toLocaleDateString();
  };

  return (
    <Drawer
      title={
        <Space style={{ width: '100%', justifyContent: 'space-between' }}>
          <Space>
            <BellOutlined />
            <span>Notifications</span>
            {notifications.length > 0 && (
              <Badge count={notifications.length} style={{ backgroundColor: '#1890ff' }} />
            )}
          </Space>
          {notifications.length > 0 && (
            <Button size="small" onClick={clearAll}>
              Clear All
            </Button>
          )}
        </Space>
      }
      placement="right"
      onClose={onClose}
      open={open}
      width={420}
      className={styles.notificationDrawer}
    >
      {notifications.length === 0 ? (
        <Empty
          image={Empty.PRESENTED_IMAGE_SIMPLE}
          description="No notifications"
          style={{ marginTop: 100 }}
        />
      ) : (
        <div>
          {notifications.map((notification) => (
            <div key={notification.id} className={styles.notificationItem}>
              <div style={{ display: 'flex', alignItems: 'flex-start', gap: 12 }}>
                <div style={{ marginTop: 4 }}>{getIcon(notification.type)}</div>
                <div style={{ flex: 1 }}>
                  <Space direction="vertical" size={4} style={{ width: '100%' }}>
                    <Text strong>{notification.message}</Text>
                    <Text type="secondary" style={{ fontSize: 12 }}>
                      {formatTime(notification.timestamp)}
                    </Text>
                  </Space>
                  {notification.description && (
                    <div style={{ fontSize: 13, color: '#666', marginTop: 8 }}>
                      {notification.description}
                    </div>
                  )}
                </div>
                <Space>
                  <Button
                    type="text"
                    size="small"
                    icon={<CopyOutlined />}
                    onClick={() => copyToClipboard(notification)}
                    title="Copy to clipboard"
                  />
                  <Button
                    type="text"
                    size="small"
                    icon={<DeleteOutlined />}
                    onClick={() => removeNotification(notification.id)}
                    title="Dismiss"
                  />
                </Space>
              </div>
            </div>
          ))}
        </div>
      )}
    </Drawer>
  );
};

export default NotificationDrawer;
