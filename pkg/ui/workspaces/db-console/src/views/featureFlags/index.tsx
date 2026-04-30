// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";
import { Helmet } from "react-helmet";
import { Link } from "react-router-dom";

import { useFeatures } from "src/hooks/useFeatures";

function FeatureFlagsPage(): React.ReactElement {
  const { features, isLoading, enableFeature, disableFeature } = useFeatures();

  return (
    <div className="section">
      <Helmet title="Feature Flags" />
      <section className="section">
        <h3 className="base-heading">Feature Flags</h3>
        <p>
          Toggle experimental DB Console features. Enabled features appear in
          the sidebar. Changes take effect cluster-wide via{" "}
          <code>dbconsole.feature_flag.*</code> cluster settings.
        </p>
      </section>
      <section className="section">
        {isLoading ? (
          <p>Loading features...</p>
        ) : features.length === 0 ? (
          <p>No features registered.</p>
        ) : (
          <table className="sort-table">
            <thead>
              <tr className="sort-table__row sort-table__row--header">
                <th className="sort-table__cell">Feature</th>
                <th className="sort-table__cell">Description</th>
                <th className="sort-table__cell">Status</th>
                <th className="sort-table__cell">Action</th>
              </tr>
            </thead>
            <tbody>
              {features.map(f => (
                <tr className="sort-table__row" key={f.name}>
                  <td className="sort-table__cell">
                    <Link to={f.route_path}>{f.title}</Link>
                  </td>
                  <td className="sort-table__cell">{f.description}</td>
                  <td className="sort-table__cell">
                    {f.enabled ? "Enabled" : "Disabled"}
                  </td>
                  <td className="sort-table__cell">
                    <button
                      onClick={() =>
                        f.enabled
                          ? disableFeature(f.name)
                          : enableFeature(f.name)
                      }
                    >
                      {f.enabled ? "Disable" : "Enable"}
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>
    </div>
  );
}

export default FeatureFlagsPage;
