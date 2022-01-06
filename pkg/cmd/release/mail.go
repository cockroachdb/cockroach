package main

import (
	"bytes"
	"fmt"
	htmltemplate "html/template"
	"net/smtp"
	"net/textproto"
	"text/template"

	"github.com/jordan-wright/email"
)

var emailSubjectTemplate = "Release {{ .Version }}"
var emailTextTemplate = `
A candidate SHA has been selected for {{ .Version }}. Proceeding to qualification shortly.

	SHA: {{ .SHA }}
	Tracking Issue: {{ .TrackingIssueURL }}
	List of changes since last release: {{ .DiffUrl }}

Thanks
Release Engineering
`
var emailHTMLTemplate = `
<html>
<body>
<p>A candidate SHA has been selected for <strong>{{ .Version }}</strong>. Proceeding to qualification shortly.</p>
<ul>
<li>SHA: <a href="https://github.com/cockroachlabs/release-staging/commit/{{ .SHA }}">{{ .SHA }}</a></li>
<li>Tracking Issue: <a href="{{ .TrackingIssueURL }}">{{ .TrackingIssue }}</a></li>
<li><a href="{{ .DiffUrl }}">List of changes</a> since last release</li>
</ul>
<p>Thanks,<br />
Release Engineering</p>
</body>
</html>
`

type emailArgs struct {
	Version          string
	SHA              string
	TrackingIssue    string
	TrackingIssueURL htmltemplate.URL
	DiffUrl          htmltemplate.URL
	// TODO: unused
	SREIssue    string
	SREIssueURL htmltemplate.URL
}

type smtpOpts struct {
	host     string
	port     int
	user     string
	password string
	from     string
	to       []string
}

// templateToText is helper function to execute a template using the text/template package
func templateToText(templateText string, args emailArgs) (string, error) {
	templ, err := template.New("").Parse(templateText)
	if err != nil {
		return "", fmt.Errorf("cannot parse template: %s", err)
	}

	var buf bytes.Buffer
	err = templ.Execute(&buf, args)
	if err != nil {
		return "", fmt.Errorf("cannot execute template: %s", err)
	}
	return buf.String(), nil
}

// templateToText is helper function to execute a template using the html/template package
func templateToHTML(templateText string, args emailArgs) (string, error) {
	templ, err := htmltemplate.New("").Parse(templateText)
	if err != nil {
		return "", fmt.Errorf("cannot parse template: %s", err)
	}

	var buf bytes.Buffer
	err = templ.Execute(&buf, args)
	if err != nil {
		return "", fmt.Errorf("cannot execute template: %s", err)
	}
	return buf.String(), nil
}

// sendmail creates and sends an email to the releases mailing list
func sendmail(args emailArgs, smtpOpts smtpOpts) error {
	text, err := templateToText(emailTextTemplate, args)
	if err != nil {
		return fmt.Errorf("cannot use text template: %w", err)
	}
	subject, err := templateToText(emailSubjectTemplate, args)
	if err != nil {
		return fmt.Errorf("cannot use subject template: %w", err)
	}
	html, err := templateToHTML(emailHTMLTemplate, args)
	if err != nil {
		return fmt.Errorf("cannot use html template: %w", err)
	}

	e := &email.Email{
		To:      smtpOpts.to,
		From:    smtpOpts.from,
		Subject: subject,
		Text:    []byte(text),
		HTML:    []byte(html),
		Headers: textproto.MIMEHeader{},
	}
	emailAuth := smtp.PlainAuth("", smtpOpts.user, smtpOpts.password, smtpOpts.host)
	addr := fmt.Sprintf("%s:%d", smtpOpts.host, smtpOpts.port)
	if err := e.Send(addr, emailAuth); err != nil {
		return fmt.Errorf("cannot send email: %w", err)
	}
	return nil
}
