// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React, { useState, useEffect } from 'react';
import {
  Modal,
  Form,
  Input,
  InputNumber,
  Select,
  Switch,
  Collapse,
  Space,
} from 'antd';
import confetti from 'canvas-confetti';
import { createCluster, getCloudProviderOptions, CloudProviderOptions } from '../../api/roachprodApi';
import { CreateClusterRequest } from '../../types/cluster';
import { useNotifications } from '../../contexts/NotificationContext';

const { Panel } = Collapse;

interface CreateClusterModalProps {
  visible: boolean;
  onClose: () => void;
  onSuccess: () => void;
}

const CreateClusterModal: React.FC<CreateClusterModalProps> = ({
  visible,
  onClose,
  onSuccess,
}) => {
  const [form] = Form.useForm();
  const [loading, setLoading] = useState(false);
  const [showAdvanced, setShowAdvanced] = useState(false);
  const { addNotification } = useNotifications();
  const [providerOptions, setProviderOptions] = useState<CloudProviderOptions | null>(null);
  const [loadingOptions, setLoadingOptions] = useState(false);

  // Watch for cloud provider changes
  const cloudProvider = Form.useWatch('cloud', form);

  // Fetch cloud provider options when provider changes
  useEffect(() => {
    const fetchOptions = async () => {
      if (!cloudProvider || cloudProvider === 'local') {
        setProviderOptions(null);
        return;
      }

      setLoadingOptions(true);
      try {
        const options = await getCloudProviderOptions(cloudProvider);
        setProviderOptions(options);
        // Reset machine type and zones when provider changes
        form.setFieldsValue({ machineType: undefined, zones: undefined });
      } catch (error) {
        console.error('Failed to fetch cloud provider options:', error);
        setProviderOptions(null);
      } finally {
        setLoadingOptions(false);
      }
    };

    fetchOptions();
  }, [cloudProvider, form]);

  const handleSubmit = async () => {
    try {
      const values = await form.validateFields();

      const request: CreateClusterRequest = {
        name: values.name,
        nodes: values.nodes,
        cloud: values.cloud,
        machineType: values.machineType,
        lifetime: `${values.lifetimeHours}h`,
        localSSD: values.localSSD,
        // Advanced options
        arch: values.arch,
        geo: values.geo,
        zones: values.zones,
        filesystem: values.filesystem,
      };

      setLoading(true);
      await createCluster(request);
      addNotification('success', `Cluster ${values.name} created successfully`);

      // Celebrate with confetti!
      confetti({
        particleCount: 100,
        spread: 70,
        origin: { y: 0.6 }
      });

      form.resetFields();
      onSuccess();
    } catch (error: unknown) {
      if (error && typeof error === 'object' && 'errorFields' in error) {
        // Form validation error
        return;
      }
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      addNotification('error', 'Failed to create cluster', errorMessage);
    } finally {
      setLoading(false);
    }
  };

  const handleCancel = () => {
    form.resetFields();
    setShowAdvanced(false);
    onClose();
  };

  return (
    <Modal
      title="Create Roachprod Cluster"
      open={visible}
      onOk={handleSubmit}
      onCancel={handleCancel}
      confirmLoading={loading}
      width={600}
      okText="Create"
    >
      <Form
        form={form}
        layout="vertical"
        initialValues={{
          nodes: 4,
          cloud: 'gce',
          lifetimeHours: 12,
          localSSD: true,
          geo: false,
        }}
      >
        {/* Basic Options */}
        <Form.Item
          label="Cluster Name"
          name="name"
          rules={[
            { required: true, message: 'Please enter a cluster name' },
            {
              pattern: /^[a-z0-9-]+$/,
              message: 'Name can only contain lowercase letters, numbers, and hyphens',
            },
          ]}
          extra="Format: <username>-<clustername>"
        >
          <Input placeholder="myuser-test" />
        </Form.Item>

        <Form.Item
          label="Cloud Provider"
          name="cloud"
          rules={[{ required: true, message: 'Please select a cloud provider' }]}
        >
          <Select options={[
            { value: 'gce', label: 'Google Cloud (GCE)' },
            { value: 'aws', label: 'Amazon Web Services (AWS)' },
            { value: 'azure', label: 'Microsoft Azure' },
            { value: 'local', label: 'Local' },
          ]} />
        </Form.Item>

        <Form.Item label="Machine Type" name="machineType">
          {cloudProvider && cloudProvider !== 'local' && providerOptions ? (
            <Select
              placeholder="Select a machine type"
              loading={loadingOptions}
              showSearch
              optionFilterProp="label"
              options={providerOptions.machineTypes.map(type => ({
                value: type.name,
                label: `${type.name} (${type.description})`,
              }))}
            />
          ) : (
            <Input placeholder="e.g., n2-standard-4 (GCE) or m6i.xlarge (AWS)" disabled={!cloudProvider || cloudProvider === 'local'} />
          )}
        </Form.Item>

        <Form.Item
          label="Number of Nodes"
          name="nodes"
          rules={[{ required: true, message: 'Please enter number of nodes' }]}
        >
          <InputNumber min={1} max={100} style={{ width: '100%' }} />
        </Form.Item>

        <Form.Item
          label="Lifetime (hours)"
          name="lifetimeHours"
          rules={[{ required: true, message: 'Please enter lifetime in hours' }]}
        >
          <InputNumber min={1} max={168} style={{ width: '100%' }} />
        </Form.Item>

        <Form.Item label="Use Local SSD" name="localSSD" valuePropName="checked">
          <Switch />
        </Form.Item>

        {/* Advanced Options */}
        <Collapse
          ghost
          onChange={(keys: string | string[]) => {
            const keyArray = Array.isArray(keys) ? keys : [keys];
            setShowAdvanced(keyArray.length > 0);
          }}
        >
          <Panel header="Advanced Options" key="advanced">
            <Form.Item label="Architecture" name="arch">
              <Select
                allowClear
                placeholder="Auto-detect"
                options={[
                  { value: 'amd64', label: 'AMD64' },
                  { value: 'arm64', label: 'ARM64' },
                  { value: 'fips', label: 'FIPS (AMD64 with OpenSSL)' },
                ]}
              />
            </Form.Item>

            <Form.Item
              label="Geo-Distributed"
              name="geo"
              valuePropName="checked"
              extra="Distribute nodes across multiple zones"
            >
              <Switch />
            </Form.Item>

            <Form.Item label="Zones" name="zones" extra="Leave empty to use defaults">
              {cloudProvider && cloudProvider !== 'local' && providerOptions ? (
                <Select
                  mode="multiple"
                  placeholder="Select zones"
                  loading={loadingOptions}
                  showSearch
                  options={providerOptions.zones.map(zone => ({
                    value: zone,
                    label: zone,
                  }))}
                />
              ) : (
                <Select
                  mode="tags"
                  placeholder="e.g., us-east1-b, us-west1-b"
                  disabled={!cloudProvider || cloudProvider === 'local'}
                />
              )}
            </Form.Item>

            <Form.Item label="Filesystem" name="filesystem">
              <Select
                allowClear
                placeholder="ext4 (default)"
                options={[
                  { value: 'ext4', label: 'ext4' },
                  { value: 'zfs', label: 'ZFS' },
                ]}
              />
            </Form.Item>
          </Panel>
        </Collapse>
      </Form>
    </Modal>
  );
};

export default CreateClusterModal;
