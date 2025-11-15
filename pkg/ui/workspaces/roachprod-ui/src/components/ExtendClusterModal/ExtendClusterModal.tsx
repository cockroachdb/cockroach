// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React, { useState } from 'react';
import { Modal, InputNumber, Space } from 'antd';

interface ExtendClusterModalProps {
  visible: boolean;
  clusterName: string;
  onClose: () => void;
  onSubmit: (hours: number) => Promise<void>;
}

const ExtendClusterModal: React.FC<ExtendClusterModalProps> = ({
  visible,
  clusterName,
  onClose,
  onSubmit,
}) => {
  const [hours, setHours] = useState(6);
  const [loading, setLoading] = useState(false);

  const handleOk = async () => {
    setLoading(true);
    try {
      await onSubmit(hours);
    } finally {
      setLoading(false);
    }
  };

  return (
    <Modal
      title={`Extend Cluster: ${clusterName}`}
      open={visible}
      onOk={handleOk}
      onCancel={onClose}
      confirmLoading={loading}
      okText="Extend"
    >
      <Space direction="vertical" style={{ width: '100%' }}>
        <p>Extend the lifetime of this cluster by:</p>
        <InputNumber
          min={1}
          max={168}
          value={hours}
          onChange={(value: number | null) => setHours(value || 6)}
          addonAfter="hours"
          style={{ width: '200px' }}
        />
        <p style={{ fontSize: 12, color: '#888', marginTop: 8 }}>
          Maximum extension: 168 hours (7 days)
        </p>
      </Space>
    </Modal>
  );
};

export default ExtendClusterModal;
