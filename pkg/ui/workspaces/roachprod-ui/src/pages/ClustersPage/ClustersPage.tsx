// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React, { useState, useEffect, useCallback, useRef } from 'react';
import { Button, Table, Popconfirm, Switch, Select, Space, Tooltip, Badge, Modal, Typography, Radio, Input } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import {
  PlusOutlined,
  ReloadOutlined,
  ClockCircleOutlined,
  CloudOutlined,
  DeleteOutlined,
  FieldTimeOutlined,
  LoginOutlined,
  AmazonOutlined,
  GoogleOutlined,
  WindowsOutlined,
  CopyOutlined,
  PlayCircleOutlined,
  StopOutlined,
  CloudDownloadOutlined,
  UploadOutlined,
  DatabaseOutlined,
  InfoCircleOutlined,
  GlobalOutlined,
  CloseCircleOutlined,
  BellOutlined,
  ClearOutlined,
  CheckCircleOutlined,
} from '@ant-design/icons';
import { ClusterInfo } from '../../types/cluster';
import {
  listClusters,
  deleteClusters,
  extendCluster,
  refreshAWSSSO,
  getSSOStatus,
  stageCluster,
  stageLocalCluster,
  startCluster,
  stopCluster,
  wipeCluster,
  getClusterInfo,
  getAdminURL,
  getClusterStatus,
  getCurrentUser,
  stageWorkload,
  startWorkload,
  stopWorkload,
  NodeStatus,
} from '../../api/roachprodApi';
import CreateClusterModal from '../../components/CreateClusterModal/CreateClusterModal';
import ExtendClusterModal from '../../components/ExtendClusterModal/ExtendClusterModal';
import SSHTerminal from '../../components/SSHTerminal/SSHTerminal';
import SQLConsole from '../../components/SQLConsole/SQLConsole';
import NotificationDrawer from '../../components/NotificationDrawer/NotificationDrawer';
import { useNotifications } from '../../contexts/NotificationContext';
import styles from '../../styles/splitPanel.module.scss';

const { Text, Paragraph } = Typography;

const ClustersPage: React.FC = () => {
  const [clusters, setClusters] = useState<ClusterInfo[]>([]);
  const [loading, setLoading] = useState(false);
  const [showMyOnly, setShowMyOnly] = useState(true);
  const [selectedCluster, setSelectedCluster] = useState<ClusterInfo | null>(null);
  const [loadingActions, setLoadingActions] = useState<Record<string, boolean>>({});
  const [createModalVisible, setCreateModalVisible] = useState(false);
  const [extendModalVisible, setExtendModalVisible] = useState(false);
  const [clusterToExtend, setClusterToExtend] = useState<ClusterInfo | null>(null);
  const [awsSsoModalVisible, setAwsSsoModalVisible] = useState(false);
  const [awsSsoOutput, setAwsSsoOutput] = useState<string>('');
  const [infoModalVisible, setInfoModalVisible] = useState(false);
  const [clusterInfo, setClusterInfo] = useState<{
    name: string;
    nodes: string[];
    pgUrls: string[];
    adminUrls: string[];
  } | null>(null);
  const [statusModalVisible, setStatusModalVisible] = useState(false);
  const [clusterStatus, setClusterStatus] = useState<{
    name: string;
    statuses: NodeStatus[];
  } | null>(null);
  const [terminalMode, setTerminalMode] = useState<'ssh' | 'sql'>('ssh');
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [pageSize, setPageSize] = useState(10);
  const [currentUser, setCurrentUser] = useState<string>('');
  const [stopModalVisible, setStopModalVisible] = useState(false);
  const [clusterToStop, setClusterToStop] = useState<string>('');
  const [stopMode, setStopMode] = useState<'all' | 'specific'>('all');
  const [specificNode, setSpecificNode] = useState<string>('');
  const [ssoTokenExpired, setSsoTokenExpired] = useState(false);
  const { addNotification, unreadCount} = useNotifications();
  const [clustersPanelWidth, setClustersPanelWidth] = useState(60); // percentage
  const isResizing = useRef(false);

  const checkSSOStatus = useCallback(async () => {
    try {
      const status = await getSSOStatus();
      console.log('SSO Status check:', status);
      console.log('Setting ssoTokenExpired to:', status.tokenExpired);
      setSsoTokenExpired(status.tokenExpired);
    } catch (error) {
      // Silently fail - don't show error notification for status check
      console.error('Failed to check SSO status:', error);
    }
  }, []);

  const fetchClusters = useCallback(async () => {
    setLoading(true);
    try {
      const data = await listClusters(showMyOnly ? 'me' : undefined);
      setClusters(data);
    } catch (error: unknown) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      addNotification('error', 'Failed to load clusters', errorMessage);
    } finally {
      setLoading(false);
      // Check SSO status after every cluster refresh
      checkSSOStatus();
    }
  }, [showMyOnly, addNotification, checkSSOStatus]);

  useEffect(() => {
    fetchClusters();
  }, [fetchClusters]);

  // Debug: log when ssoTokenExpired changes
  useEffect(() => {
    console.log('ssoTokenExpired state changed to:', ssoTokenExpired);
  }, [ssoTokenExpired]);

  useEffect(() => {
    const fetchUser = async () => {
      try {
        const username = await getCurrentUser();
        setCurrentUser(username);
      } catch (error: unknown) {
        const errorMessage = error instanceof Error ? error.message : 'Unknown error';
        addNotification('error', 'Failed to get current user', errorMessage);
      }
    };
    fetchUser();
  }, [addNotification]);

  // Handle panel resize
  const handleMouseDown = useCallback(() => {
    isResizing.current = true;
  }, []);

  const handleMouseMove = useCallback((e: MouseEvent) => {
    if (!isResizing.current) return;

    const containerWidth = window.innerWidth;
    const newWidth = (e.clientX / containerWidth) * 100;

    // Constrain between 30% and 80%
    if (newWidth >= 30 && newWidth <= 80) {
      setClustersPanelWidth(newWidth);
    }
  }, []);

  const handleMouseUp = useCallback(() => {
    isResizing.current = false;
  }, []);

  useEffect(() => {
    document.addEventListener('mousemove', handleMouseMove);
    document.addEventListener('mouseup', handleMouseUp);

    return () => {
      document.removeEventListener('mousemove', handleMouseMove);
      document.removeEventListener('mouseup', handleMouseUp);
    };
  }, [handleMouseMove, handleMouseUp]);

  const handleDelete = async (clusterName: string) => {
    try {
      await deleteClusters([clusterName]);
      addNotification('success', `Cluster ${clusterName} deleted successfully`);
      if (selectedCluster?.name === clusterName) {
        setSelectedCluster(null);
      }
      fetchClusters();
    } catch (error: unknown) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      addNotification('error', 'Failed to delete cluster', errorMessage);
    }
  };

  const handleExtend = (cluster: ClusterInfo) => {
    setClusterToExtend(cluster);
    setExtendModalVisible(true);
  };

  const handleExtendSubmit = async (hours: number) => {
    if (!clusterToExtend) return;

    try {
      await extendCluster(clusterToExtend.name, `${hours}h`);
      addNotification('success', `Cluster ${clusterToExtend.name} extended by ${hours} hours`);
      setExtendModalVisible(false);
      setClusterToExtend(null);
    } catch (error: unknown) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      addNotification('error', 'Failed to extend cluster', errorMessage);
    }
  };

  const handleSSH = (cluster: ClusterInfo) => {
    setTerminalMode('ssh');
    setSelectedCluster(cluster);
  };

  const handleSQL = (cluster: ClusterInfo) => {
    setTerminalMode('sql');
    setSelectedCluster(cluster);
  };

  const handleStage = async (clusterName: string) => {
    const key = `stage-${clusterName}`;
    setLoadingActions(prev => ({ ...prev, [key]: true }));
    try {
      await stageCluster(clusterName);
      addNotification('success', `Staging binaries on cluster ${clusterName}`);
    } catch (error: unknown) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      addNotification('error', 'Failed to stage cluster', errorMessage);
    } finally {
      setLoadingActions(prev => ({ ...prev, [key]: false }));
    }
  };

  const handleStageLocal = async (clusterName: string) => {
    const key = `stage-local-${clusterName}`;
    setLoadingActions(prev => ({ ...prev, [key]: true }));
    try {
      await stageLocalCluster(clusterName);
      addNotification('success', `Staging local binary on cluster ${clusterName}`);
    } catch (error: unknown) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      addNotification('error', 'Failed to stage local binary', errorMessage);
    } finally {
      setLoadingActions(prev => ({ ...prev, [key]: false }));
    }
  };

  const handleStart = async (clusterName: string) => {
    const key = `start-${clusterName}`;
    setLoadingActions(prev => ({ ...prev, [key]: true }));
    try {
      await startCluster(clusterName);
      addNotification('success', `Started cockroach on cluster ${clusterName}`);
    } catch (error: unknown) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      addNotification('error', 'Failed to start cluster', errorMessage);
    } finally {
      setLoadingActions(prev => ({ ...prev, [key]: false }));
    }
  };

  const handleStop = (clusterName: string) => {
    setClusterToStop(clusterName);
    setStopMode('all');
    setSpecificNode('');
    setStopModalVisible(true);
  };

  const handleStopConfirm = async () => {
    // Validate node number if specific mode is selected
    if (stopMode === 'specific') {
      if (!specificNode || specificNode.trim() === '') {
        addNotification('error', 'Invalid input', 'Please enter a node number');
        return;
      }
      // Validate that it's a number or range
      const nodePattern = /^(\d+)(-\d+)?$/;
      if (!nodePattern.test(specificNode.trim())) {
        addNotification('error', 'Invalid node format', 'Please enter a valid node number (e.g., "3") or range (e.g., "1-3")');
        return;
      }
    }

    const key = `stop-${clusterToStop}`;
    setLoadingActions(prev => ({ ...prev, [key]: true }));
    setStopModalVisible(false);

    try {
      const nodeParam = stopMode === 'specific' ? specificNode.trim() : undefined;
      await stopCluster(clusterToStop, nodeParam);

      const successMsg = stopMode === 'specific'
        ? `Stopped cockroach on node(s) ${specificNode} of cluster ${clusterToStop}`
        : `Stopped cockroach on cluster ${clusterToStop}`;
      addNotification('success', successMsg);
    } catch (error: unknown) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      addNotification('error', 'Failed to stop cluster', errorMessage);
    } finally {
      setLoadingActions(prev => ({ ...prev, [key]: false }));
    }
  };

  const handleWipe = async (clusterName: string) => {
    const key = `wipe-${clusterName}`;
    setLoadingActions(prev => ({ ...prev, [key]: true }));
    try {
      await wipeCluster(clusterName);
      addNotification('success', `Wiped cluster ${clusterName}`);
    } catch (error: unknown) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      addNotification('error', 'Failed to wipe cluster', errorMessage);
    } finally {
      setLoadingActions(prev => ({ ...prev, [key]: false }));
    }
  };

  const handleStageWorkload = async (clusterName: string) => {
    const key = `stage-workload-${clusterName}`;
    setLoadingActions(prev => ({ ...prev, [key]: true }));
    try {
      await stageWorkload(clusterName);
      addNotification('success', `Staging workload on cluster ${clusterName}`);
    } catch (error: unknown) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      addNotification('error', 'Failed to stage workload', errorMessage);
    } finally {
      setLoadingActions(prev => ({ ...prev, [key]: false }));
    }
  };

  const handleStartWorkload = async (clusterName: string) => {
    const key = `start-workload-${clusterName}`;
    setLoadingActions(prev => ({ ...prev, [key]: true }));
    try {
      await startWorkload(clusterName);
      addNotification('success', `Started TPCC workload on cluster ${clusterName}`);
    } catch (error: unknown) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      addNotification('error', 'Failed to start workload', errorMessage);
    } finally {
      setLoadingActions(prev => ({ ...prev, [key]: false }));
    }
  };

  const handleStopWorkload = async (clusterName: string) => {
    const key = `stop-workload-${clusterName}`;
    setLoadingActions(prev => ({ ...prev, [key]: true }));
    try {
      await stopWorkload(clusterName);
      addNotification('success', `Stopped workload on cluster ${clusterName}`);
    } catch (error: unknown) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      addNotification('error', 'Failed to stop workload', errorMessage);
    } finally {
      setLoadingActions(prev => ({ ...prev, [key]: false }));
    }
  };

  const handleShowInfo = async (cluster: ClusterInfo) => {
    const actionKey = `info-${cluster.name}`;
    setLoadingActions(prev => ({ ...prev, [actionKey]: true }));
    try {
      const info = await getClusterInfo(cluster.name);
      setClusterInfo(info);
      setInfoModalVisible(true);
    } catch (error: unknown) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      addNotification('error', 'Failed to get cluster info', errorMessage);
    } finally {
      setLoadingActions(prev => ({ ...prev, [actionKey]: false }));
    }
  };

  const handleOpenUI = async (cluster: ClusterInfo) => {
    const actionKey = `openui-${cluster.name}`;
    setLoadingActions(prev => ({ ...prev, [actionKey]: true }));
    try {
      const result = await getAdminURL(cluster.name);
      window.open(result.url, '_blank');
    } catch (error: unknown) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      addNotification('error', 'Failed to open UI', errorMessage);
    } finally {
      setLoadingActions(prev => ({ ...prev, [actionKey]: false }));
    }
  };

  const handleShowStatus = async (cluster: ClusterInfo) => {
    const actionKey = `status-${cluster.name}`;
    setLoadingActions(prev => ({ ...prev, [actionKey]: true }));
    try {
      const result = await getClusterStatus(cluster.name);
      setClusterStatus({
        name: cluster.name,
        statuses: result.statuses,
      });
      setStatusModalVisible(true);
    } catch (error: unknown) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      addNotification('error', 'Failed to get cluster status', errorMessage);
    } finally {
      setLoadingActions(prev => ({ ...prev, [actionKey]: false }));
    }
  };

  const handleCreateSuccess = () => {
    setCreateModalVisible(false);
    fetchClusters();
  };

  const handleAWSSSORefresh = async () => {
    try {
      const response = await refreshAWSSSO();
      if (response.output) {
        setAwsSsoOutput(response.output);
        setAwsSsoModalVisible(true);

        // Auto-copy verification code to clipboard
        const codeMatch = response.output.match(/code:\s*\n\s*([A-Z0-9-]+)/i);
        if (codeMatch) {
          navigator.clipboard.writeText(codeMatch[1]);
          addNotification('success', 'Verification code copied to clipboard',
            `Code: ${codeMatch[1]}`);
        }
      }

      // Check SSO status after refresh
      checkSSOStatus();
    } catch (error: unknown) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      addNotification('error', 'Failed to refresh AWS SSO', errorMessage);
    }
  };

  const columns: ColumnsType<ClusterInfo> = [
    {
      title: 'Cluster Name',
      dataIndex: 'name',
      key: 'name',
      width: 200,
      sorter: (a, b) => a.name.localeCompare(b.name),
    },
    {
      title: 'Nodes',
      dataIndex: 'nodes',
      key: 'nodes',
      width: 70,
      sorter: (a, b) => a.nodes - b.nodes,
      render: (nodes: number, record: ClusterInfo) => {
        // Display as "N-1+1" for workload clusters (e.g., "3+1" for 4 nodes)
        if (record.name.endsWith('-with-workload')) {
          return <span>{nodes - 1}+1</span>;
        }
        return <span>{nodes}</span>;
      },
    },
    {
      title: 'Cloud',
      dataIndex: 'cloud',
      key: 'cloud',
      width: 70,
      render: (cloud: string) => {
        const cloudLower = cloud.toLowerCase();
        let icon = <CloudOutlined style={{ fontSize: 20 }} />;

        if (cloudLower.includes('gce') || cloudLower.includes('gcp') || cloudLower.includes('google')) {
          icon = <GoogleOutlined style={{ fontSize: 20, color: '#4285F4' }} />;
        } else if (cloudLower.includes('aws') || cloudLower.includes('amazon')) {
          icon = <AmazonOutlined style={{ fontSize: 20, color: '#FF9900' }} />;
        } else if (cloudLower.includes('azure')) {
          icon = <WindowsOutlined style={{ fontSize: 20, color: '#0078D4' }} />;
        }

        return <Tooltip title={cloud}>{icon}</Tooltip>;
      },
    },
    {
      title: 'Machine Type',
      dataIndex: 'machineType',
      key: 'machineType',
      width: 180,
      render: (type: string) => <span style={{ fontSize: 13, color: '#666' }}>{type}</span>,
    },
    {
      title: 'Lifetime Remaining',
      dataIndex: 'lifetimeRemaining',
      key: 'lifetimeRemaining',
      width: 180,
      render: (text: string) => {
        const isExpired = text === 'expired';
        const isLocal = text === '-';

        if (isLocal) {
          return <Badge status="default" text="Local" />;
        }

        if (isExpired) {
          return <Badge status="error" text="Expired" />;
        }

        return <Badge status="success" text={text} />;
      },
    },
    {
      title: 'Created',
      dataIndex: 'created',
      key: 'created',
      width: 180,
      render: (text: string) => {
        const date = new Date(text);
        const now = new Date();
        const diffDays = Math.floor((now.getTime() - date.getTime()) / (1000 * 60 * 60 * 24));
        const timeStr = date.toLocaleString();

        return (
          <Tooltip title={timeStr}>
            <span style={{ color: '#666' }}>
              {diffDays === 0 ? 'Today' : diffDays === 1 ? 'Yesterday' : `${diffDays}d ago`}
            </span>
          </Tooltip>
        );
      },
      sorter: (a, b) => new Date(a.created).getTime() - new Date(b.created).getTime(),
    },
    {
      title: 'Actions',
      key: 'actions',
      width: 450,
      render: (_: any, record: ClusterInfo) => {
        // Check if user owns this cluster
        // currentUser is a comma-separated list of account names
        const userAccounts = currentUser.split(',');
        const isOwner = userAccounts.indexOf(record.owner) !== -1;
        return (
        <Space size={2} wrap>
          {/* Group 1: Stage and start operations */}
          <Tooltip title={!isOwner ? "You don't own this cluster" : "Stage cockroach binaries"} open={loadingActions[`stage-${record.name}`] ? false : undefined}>
            <Button size="small" icon={<CloudDownloadOutlined />} onClick={() => handleStage(record.name)} loading={loadingActions[`stage-${record.name}`]} disabled={!isOwner} />
          </Tooltip>
          <Tooltip title={!isOwner ? "You don't own this cluster" : "Stage local binary (~/artifacts/cockroach)"} open={loadingActions[`stage-local-${record.name}`] ? false : undefined}>
            <Button size="small" icon={<UploadOutlined />} onClick={() => handleStageLocal(record.name)} loading={loadingActions[`stage-local-${record.name}`]} disabled={!isOwner} />
          </Tooltip>
          <Tooltip title={!isOwner ? "You don't own this cluster" : "Start cockroach"} open={loadingActions[`start-${record.name}`] ? false : undefined}>
            <Button size="small" icon={<PlayCircleOutlined />} onClick={() => handleStart(record.name)} loading={loadingActions[`start-${record.name}`]} disabled={!isOwner} />
          </Tooltip>
          {record.name.endsWith('-with-workload') && (
            <>
              <Tooltip title={!isOwner ? "You don't own this cluster" : "Stage workload binary"} open={loadingActions[`stage-workload-${record.name}`] ? false : undefined}>
                <Button size="small" icon={<CloudDownloadOutlined />} onClick={() => handleStageWorkload(record.name)} loading={loadingActions[`stage-workload-${record.name}`]} disabled={!isOwner} type="dashed" />
              </Tooltip>
              <Tooltip title={!isOwner ? "You don't own this cluster" : "Start TPCC workload"} open={loadingActions[`start-workload-${record.name}`] ? false : undefined}>
                <Button size="small" icon={<PlayCircleOutlined />} onClick={() => handleStartWorkload(record.name)} loading={loadingActions[`start-workload-${record.name}`]} disabled={!isOwner} type="dashed" />
              </Tooltip>
            </>
          )}

          {/* Group 2: Access tools */}
          <Tooltip title="SQL Console" open={loadingActions[`sql-${record.name}`] ? false : undefined}>
            <Button size="small" icon={<DatabaseOutlined />} onClick={() => handleSQL(record)} loading={loadingActions[`sql-${record.name}`]} />
          </Tooltip>
          <Tooltip title="SSH into cluster" open={loadingActions[`ssh-${record.name}`] ? false : undefined}>
            <Button size="small" icon={<LoginOutlined />} onClick={() => handleSSH(record)} loading={loadingActions[`ssh-${record.name}`]} />
          </Tooltip>
          <Tooltip title="Open Cockroach UI" open={loadingActions[`openui-${record.name}`] ? false : undefined}>
            <Button size="small" icon={<GlobalOutlined />} onClick={() => handleOpenUI(record)} loading={loadingActions[`openui-${record.name}`]} />
          </Tooltip>

          {/* Group 3: Information and management */}
          <Tooltip title="Cluster Status" open={loadingActions[`status-${record.name}`] ? false : undefined}>
            <Button size="small" icon={<CheckCircleOutlined />} onClick={() => handleShowStatus(record)} loading={loadingActions[`status-${record.name}`]} />
          </Tooltip>
          <Tooltip title="Cluster Info" open={loadingActions[`info-${record.name}`] ? false : undefined}>
            <Button size="small" icon={<InfoCircleOutlined />} onClick={() => handleShowInfo(record)} loading={loadingActions[`info-${record.name}`]} />
          </Tooltip>
          <Tooltip title="Extend lifetime" open={loadingActions[`extend-${record.name}`] ? false : undefined}>
            <Button size="small" icon={<FieldTimeOutlined />} onClick={() => handleExtend(record)} loading={loadingActions[`extend-${record.name}`]} />
          </Tooltip>

          {/* Group 4: Destructive operations */}
          <Tooltip title={!isOwner ? "You don't own this cluster" : "Stop cockroach"} open={loadingActions[`stop-${record.name}`] ? false : undefined}>
            <Button size="small" icon={<StopOutlined />} onClick={() => handleStop(record.name)} loading={loadingActions[`stop-${record.name}`]} danger disabled={!isOwner} />
          </Tooltip>
          {record.name.endsWith('-with-workload') && (
            <Tooltip title={!isOwner ? "You don't own this cluster" : "Stop workload"} open={loadingActions[`stop-workload-${record.name}`] ? false : undefined}>
              <Button size="small" icon={<StopOutlined />} onClick={() => handleStopWorkload(record.name)} loading={loadingActions[`stop-workload-${record.name}`]} disabled={!isOwner} danger type="dashed" />
            </Tooltip>
          )}
          <Popconfirm
            title={`Wipe cluster ${record.name}?`}
            description="This will wipe all data on the cluster."
            onConfirm={() => handleWipe(record.name)}
            okText="Wipe"
            cancelText="Cancel"
            okButtonProps={{ danger: true }}
            disabled={!isOwner}
          >
            <Tooltip title={!isOwner ? "You don't own this cluster" : "Wipe cluster"} open={loadingActions[`wipe-${record.name}`] ? false : undefined}>
              <Button size="small" icon={<ClearOutlined />} danger loading={loadingActions[`wipe-${record.name}`]} disabled={!isOwner} />
            </Tooltip>
          </Popconfirm>
          <Popconfirm
            title={`Delete cluster ${record.name}?`}
            description="This action cannot be undone."
            onConfirm={() => handleDelete(record.name)}
            okText="Delete"
            cancelText="Cancel"
            okButtonProps={{ danger: true }}
            disabled={!isOwner}
          >
            <Tooltip title={!isOwner ? "You don't own this cluster" : "Delete cluster"}>
              <Button size="small" icon={<DeleteOutlined />} danger disabled={!isOwner} />
            </Tooltip>
          </Popconfirm>
        </Space>
        );
      },
    },
  ];

  return (
    <div className={styles.splitContainer}>
      {/* Left Panel - Cluster List */}
      <div className={styles.clustersPanel} style={{ width: `${clustersPanelWidth}%` }}>
        <div className={styles.clustersPanelHeader}>
          <div className={styles.bannerContainer}>
            <img
              src="/roachprod-wonderland.png"
              alt="Roachprod Wonderland"
              className={styles.bannerImage}
            />
            <div className={styles.filterControls}>
            <Space>
              <span style={{ color: '#ffffff' }}>Show all clusters:</span>
              <Switch checked={!showMyOnly} onChange={(checked: boolean) => setShowMyOnly(!checked)} />
              <Button
                icon={<ReloadOutlined />}
                onClick={fetchClusters}
                loading={loading}
              >
                Refresh
              </Button>
              <Button
                type="primary"
                icon={<PlusOutlined />}
                onClick={() => setCreateModalVisible(true)}
              >
                Create Cluster
              </Button>
              <Tooltip title={ssoTokenExpired ? "AWS SSO token expired - click to refresh" : "Refresh AWS SSO credentials"}>
                <Button
                  icon={<AmazonOutlined />}
                  onClick={handleAWSSSORefresh}
                  danger={ssoTokenExpired}
                  type={ssoTokenExpired ? "primary" : "default"}
                />
              </Tooltip>
              <Tooltip title="Notifications">
                <Badge count={unreadCount} offset={[-4, 4]}>
                  <Button
                    icon={<BellOutlined />}
                    onClick={() => setDrawerOpen(true)}
                  />
                </Badge>
              </Tooltip>
            </Space>
            </div>
          </div>
        </div>
        <div className={styles.clustersPanelContent}>
          <Table
            columns={columns}
            dataSource={clusters}
            loading={loading}
            rowKey="name"
            pagination={{
              pageSize: pageSize,
              showSizeChanger: true,
              showTotal: (total: number) => `Total ${total} clusters`,
              onShowSizeChange: (_current: number, size: number) => setPageSize(size),
            }}
            size="small"
          />
        </div>
      </div>

      {/* Resize Handle */}
      <div className={styles.resizeHandle} onMouseDown={handleMouseDown} />

      {/* Right Panel - Terminal (SSH/SQL) */}
      <div className={styles.terminalPanel}>
        {selectedCluster ? (
          terminalMode === 'sql' ? (
            <SQLConsole cluster={selectedCluster} onClose={() => setSelectedCluster(null)} />
          ) : (
            <SSHTerminal cluster={selectedCluster} onClose={() => setSelectedCluster(null)} />
          )
        ) : (
          <div className={styles.emptyTerminal}>
            <div>
              <p>Select a cluster to open a terminal session</p>
              <p style={{ fontSize: 12, marginTop: 8 }}>
                Click on SSH or SQL button to connect
              </p>
            </div>
          </div>
        )}
      </div>

      {/* Modals */}
      <CreateClusterModal
        visible={createModalVisible}
        onClose={() => setCreateModalVisible(false)}
        onSuccess={handleCreateSuccess}
      />

      <ExtendClusterModal
        visible={extendModalVisible}
        clusterName={clusterToExtend?.name || ''}
        onClose={() => {
          setExtendModalVisible(false);
          setClusterToExtend(null);
        }}
        onSubmit={handleExtendSubmit}
      />

      <Modal
        title="AWS SSO Login"
        open={awsSsoModalVisible}
        onCancel={() => setAwsSsoModalVisible(false)}
        footer={[
          <Button key="close" type="primary" onClick={() => setAwsSsoModalVisible(false)}>
            Close
          </Button>
        ]}
        width={600}
      >
        <div style={{
          fontFamily: 'monospace',
          whiteSpace: 'pre-wrap',
          fontSize: 13,
          backgroundColor: '#fafafa',
          padding: 12,
          borderRadius: 4,
          maxHeight: 300,
          overflow: 'auto'
        }}>
          {awsSsoOutput}
        </div>
      </Modal>

      {/* Cluster Info Modal */}
      <Modal
        title={`Cluster Info: ${clusterInfo?.name || ''}`}
        open={infoModalVisible}
        onCancel={() => setInfoModalVisible(false)}
        footer={[
          <Button key="close" onClick={() => setInfoModalVisible(false)}>
            Close
          </Button>
        ]}
        width={800}
      >
        {clusterInfo && (
          <div>
            <div style={{ marginBottom: 24 }}>
              <Text strong style={{ fontSize: 16 }}>Node IPs:</Text>
              <div style={{ marginTop: 8 }}>
                {clusterInfo.nodes.map((ip, idx) => (
                  <div key={idx} style={{ marginBottom: 4 }}>
                    <Text code>Node {idx + 1}: {ip}</Text>
                    <Button
                      size="small"
                      icon={<CopyOutlined />}
                      onClick={() => {
                        navigator.clipboard.writeText(ip);
                        addNotification('success', `Copied IP for node ${idx + 1}`);
                      }}
                      style={{ marginLeft: 8 }}
                    />
                  </div>
                ))}
              </div>
            </div>

            <div style={{ marginBottom: 24 }}>
              <Text strong style={{ fontSize: 16 }}>PostgreSQL URLs:</Text>
              <div style={{ marginTop: 8 }}>
                {clusterInfo.pgUrls.map((url, idx) => (
                  <div key={idx} style={{ marginBottom: 8 }}>
                    <Paragraph
                      code
                      copyable={{
                        text: url,
                        onCopy: () => addNotification('success', `Copied PG URL for node ${idx + 1}`),
                      }}
                      style={{ marginBottom: 0 }}
                    >
                      {url}
                    </Paragraph>
                  </div>
                ))}
              </div>
            </div>

            <div>
              <Text strong style={{ fontSize: 16 }}>Admin UI URLs:</Text>
              <div style={{ marginTop: 8 }}>
                {clusterInfo.adminUrls.map((url, idx) => (
                  <div key={idx} style={{ marginBottom: 8, display: 'flex', alignItems: 'center' }}>
                    <Paragraph
                      code
                      copyable={{
                        text: url,
                        onCopy: () => addNotification('success', `Copied Admin URL for node ${idx + 1}`),
                      }}
                      style={{ marginBottom: 0, flex: 1 }}
                    >
                      {url}
                    </Paragraph>
                    <Button
                      size="small"
                      icon={<GlobalOutlined />}
                      onClick={() => window.open(url, '_blank')}
                      style={{ marginLeft: 8 }}
                    >
                      Open
                    </Button>
                  </div>
                ))}
              </div>
            </div>
          </div>
        )}
      </Modal>

      {/* Cluster Status Modal */}
      <Modal
        title={`Cluster Status: ${clusterStatus?.name || ''}`}
        open={statusModalVisible}
        onCancel={() => setStatusModalVisible(false)}
        footer={[
          <Button key="close" onClick={() => setStatusModalVisible(false)}>
            Close
          </Button>
        ]}
        width={700}
      >
        {clusterStatus && (
          <div>
            {clusterStatus.statuses.map((status) => (
              <div
                key={status.nodeId}
                style={{
                  padding: 12,
                  marginBottom: 8,
                  border: '1px solid #d9d9d9',
                  borderRadius: 4,
                  backgroundColor: status.running ? '#f6ffed' : '#fff1f0',
                }}
              >
                <div style={{ display: 'flex', alignItems: 'center', marginBottom: 8 }}>
                  <Text strong style={{ fontSize: 14, marginRight: 12 }}>
                    Node {status.nodeId}
                  </Text>
                  <Badge
                    status={status.running ? 'success' : 'error'}
                    text={status.running ? 'Running' : 'Stopped'}
                  />
                </div>
                {status.running && (
                  <>
                    <div style={{ marginBottom: 4 }}>
                      <Text type="secondary">Version: </Text>
                      <Text code>{status.version || 'Unknown'}</Text>
                    </div>
                    <div style={{ marginBottom: 4 }}>
                      <Text type="secondary">PID: </Text>
                      <Text code>{status.pid}</Text>
                    </div>
                  </>
                )}
                {status.err && (
                  <div style={{ marginTop: 8 }}>
                    <Text type="danger">{status.err}</Text>
                  </div>
                )}
              </div>
            ))}
          </div>
        )}
      </Modal>

      {/* Stop Cluster Modal */}
      <Modal
        title={`Stop Cluster: ${clusterToStop}`}
        open={stopModalVisible}
        onOk={handleStopConfirm}
        onCancel={() => setStopModalVisible(false)}
        okText="Stop"
        okButtonProps={{ danger: true }}
      >
        <Space direction="vertical" style={{ width: '100%' }}>
          <Text>Choose stop mode:</Text>
          <Radio.Group value={stopMode} onChange={(e: any) => setStopMode(e.target.value)}>
            <Space direction="vertical">
              <Radio value="all">Stop all nodes</Radio>
              <Radio value="specific">
                Stop specific node
                {stopMode === 'specific' && (
                  <Input
                    placeholder="Enter node number"
                    value={specificNode}
                    onChange={(e: React.ChangeEvent<HTMLInputElement>) => setSpecificNode(e.target.value)}
                    style={{ marginLeft: 8, width: 150 }}
                  />
                )}
              </Radio>
            </Space>
          </Radio.Group>
        </Space>
      </Modal>

      <NotificationDrawer open={drawerOpen} onClose={() => setDrawerOpen(false)} />
    </div>
  );
};

export default ClustersPage;
