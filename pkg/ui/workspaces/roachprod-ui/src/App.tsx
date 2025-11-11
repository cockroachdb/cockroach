// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from 'react';
import { ConfigProvider, theme, message } from 'antd';
import ClustersPage from './pages/ClustersPage/ClustersPage';
import { NotificationProvider } from './contexts/NotificationContext';
import BackendConsole from './components/BackendConsole/BackendConsole';
import './styles/app.scss';

// Configure message globally to show close icon
message.config({
  top: 24,
  maxCount: 3,
});

const AppContent: React.FC = () => {
  return (
    <>
      <div className="roachprod-app">
        <ClustersPage />
      </div>
      <BackendConsole />
    </>
  );
};

const App: React.FC = () => {
  // Always use light mode for now with Inter font
  return (
    <ConfigProvider
      theme={{
        algorithm: theme.defaultAlgorithm,
        token: {
          fontFamily: "'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif",
          fontSize: 14,
          borderRadius: 8,
        },
      }}
    >
      <NotificationProvider>
        <AppContent />
      </NotificationProvider>
    </ConfigProvider>
  );
};

export default App;
