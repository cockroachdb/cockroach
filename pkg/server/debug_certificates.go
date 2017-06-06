// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package server

import (
	"crypto/ecdsa"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"html/template"
	"net/http"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
)

// Returns an HTML page displaying information about the certificates currently
// loaded on the requested node.
func (s *statusServer) handleDebugCertificates(w http.ResponseWriter, r *http.Request) {
	ctx := s.AnnotateCtx(r.Context())
	w.Header().Add("Content-type", "text/html")
	nodeIDString := r.URL.Query().Get("node_id")

	nodeID, _, err := s.parseNodeID(nodeIDString)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	req := &serverpb.CertificatesRequest{NodeId: nodeIDString}
	certificatesResponse, err := s.Certificates(ctx, req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	webData := prepareCertificateWebData(nodeID, certificatesResponse)

	t, err := template.New("webpage").Parse(debugCertificatesTemplate)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := t.Execute(w, webData); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

type certificatesWebData struct {
	NodeID       roachpb.NodeID
	Certificates []perCertificateWebData
}

type perCertificateWebData struct {
	CertificateType string
	certs           []*x509.Certificate
	CertFields      [][]stringPair
	ErrorMessage    string
}

type stringPair struct {
	Key   string
	Value string
}

func prepareCertificateWebData(
	nodeID roachpb.NodeID, response *serverpb.CertificatesResponse,
) *certificatesWebData {
	ret := &certificatesWebData{
		NodeID:       nodeID,
		Certificates: make([]perCertificateWebData, len(response.Certificates)),
	}

	for i, c := range response.Certificates {
		d := perCertificateWebData{}

		switch c.Type {
		case serverpb.CertificateDetails_CA:
			d.CertificateType = "Certificate Authority"
		case serverpb.CertificateDetails_NODE:
			d.CertificateType = "Node"
		default:
			d.CertificateType = fmt.Sprintf("unknown type: %s", c.Type.String())
		}

		d.ErrorMessage = c.ErrorMessage
		if c.ErrorMessage == "" {
			certs, err := security.PEMContentsToX509(c.Data)
			if err != nil {
				d.ErrorMessage = err.Error()
			} else {
				d.certs = certs
				extractUsefulCertificateFields(&d)
			}
		}
		ret.Certificates[i] = d
	}

	return ret
}

func extractUsefulCertificateFields(data *perCertificateWebData) {
	data.CertFields = make([][]stringPair, len(data.certs))
	for i, c := range data.certs {
		addresses := c.DNSNames
		for _, ip := range c.IPAddresses {
			addresses = append(addresses, ip.String())
		}

		formatNames := func(p pkix.Name) string {
			return fmt.Sprintf("CommonName=%s, Organization=%s", p.CommonName, strings.Join(p.Organization, ","))
		}

		extKeyUsage := make([]string, len(c.ExtKeyUsage))
		for i, eku := range c.ExtKeyUsage {
			extKeyUsage[i] = security.ExtKeyUsageToString(eku)
		}

		var pubKeyInfo string
		if rsaPub, ok := c.PublicKey.(*rsa.PublicKey); ok {
			pubKeyInfo = fmt.Sprintf("%d bit RSA", rsaPub.N.BitLen())
		} else if ecdsaPub, ok := c.PublicKey.(*ecdsa.PublicKey); ok {
			pubKeyInfo = fmt.Sprintf("%d bit ECDSA", ecdsaPub.Params().BitSize)
		} else {
			// go's x509 library does not support other types (so far).
			pubKeyInfo = fmt.Sprintf("unknown key type %T", c.PublicKey)
		}

		data.CertFields[i] = []stringPair{
			{"Issuer", formatNames(c.Issuer)},
			{"Subject", formatNames(c.Subject)},
			{"Valid From", c.NotBefore.Format("2006-01-02 15:04:05 MST")},
			{"Valid Until", c.NotAfter.Format("2006-01-02 15:04:05 MST")},
			{"Addresses", strings.Join(addresses, ", ")},
			{"Signature Algorithm", c.SignatureAlgorithm.String()},
			{"Public Key", pubKeyInfo},
			{"Key Usage", strings.Join(security.KeyUsageToString(c.KeyUsage), ", ")},
			{"Extended Key Usage", strings.Join(extKeyUsage, ", ")},
		}
	}
}

const debugCertificatesTemplate = `
<!DOCTYPE html>
<HTML>
  <HEAD>
    <META CHARSET="UTF-8"/>
    <TITLE>Node {{.NodeID}} certificates</TITLE>
    <STYLE>
      body {
        font-family: "Helvetica Neue", Helvetica, Arial;
        font-size: 14px;
        line-height: 20px;
        font-weight: 400;
        color: #3b3b3b;
        -webkit-font-smoothing: antialiased;
        font-smoothing: antialiased;
        background: #e4e4e4;
      }
      .wrapper {
        margin: 0 auto;
        padding: 0 40px;
      }
      .table {
        margin: 0 0 40px 0;
        display: table;
        width: 100%;
      }
      .row {
        display: table-row;
        background: #f6f6f6;
      }
      .cell {
        padding: 6px 12px;
        display: table-cell;
        height: 20px;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
        max-width: 80%;
        border-width: 1px 1px 0 0;
        border-color: rgba(0, 0, 0, 0.1);
        border-style: solid;
      }
      .header.cell{
        font-weight: 900;
        color: #ffffff;
        background: #2980b9;
        text-overflow: clip;
        border: none;
        width: 1px;
        text-align: right;
      }

    </STYLE>
  </HEAD>
  <BODY>
    <DIV CLASS="wrapper">
      <H1>Node {{.NodeID}} certificates</H1>
      {{- range .Certificates}}
      <DIV CLASS="table">
        <DIV CLASS="row">
          <DIV CLASS="header cell">Type</DIV>
          <DIV CLASS="cell">{{.CertificateType}}</DIV>
        </DIV>
        {{- if not (eq .ErrorMessage "")}}
          <DIV CLASS="row">
            <DIV CLASS="header cell">Error</DIV>
            <DIV CLASS="cell">{{.ErrorMessage}}</DIV>
          </DIV>
        {{- else}}
          {{- range $i, $cert := .CertFields}}
            {{- if gt $i 0}}
              <DIV CLASS="row">
                <DIV CLASS="header cell"></DIV>
                <DIV CLASS="cell"></DIV>
              </DIV>
            {{- end}}
            <DIV CLASS="row">
              <DIV CLASS="header cell">Cert ID</DIV>
              <DIV CLASS="cell">{{$i}}</DIV>
            </DIV>
            {{- range .}}
              <DIV CLASS="row">
                <DIV CLASS="header cell">{{.Key}}</DIV>
                <DIV CLASS="cell">{{.Value}}</DIV>
              </DIV>
            {{- end}}
          {{- end}}
        {{- end}}
      </DIV>
      {{- end}}
    </DIV>
  </BODY>
</HTML>
`
