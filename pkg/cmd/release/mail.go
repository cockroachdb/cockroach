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
	Tracking Issue: {{ .JiraUrl }}
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
<li>Tracking Issue: <a href="{{ .JiraUrl }}">{{ .JiraTicket }}</a></li>
<li><a href="{{ .DiffUrl }}">List of changes</a> since last release</li>
</ul>
<p>Thanks,<br />
Release Engineering</p>
</body>
</html>
`

type emailArgs struct {
	To         []string
	Version    string
	SHA        string
	JiraTicket string
	JiraUrl    string
	DiffUrl    htmltemplate.URL
}

type smtpOpts struct {
	host     string
	port     int
	user     string
	password string
	from     string
}

func templateToText(templateText string, args emailArgs) (string, error) {
	templ, err := template.New("").Parse(templateText)
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer
	err = templ.Execute(&buf, args)
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}

func templateToHTML(templateText string, args emailArgs) (string, error) {
	templ, err := htmltemplate.New("").Parse(templateText)
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer
	err = templ.Execute(&buf, args)
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}

func sendEmailSMTP(args emailArgs, smtpOpts smtpOpts) error {
	text, err := templateToText(emailTextTemplate, args)
	if err != nil {
		return err
	}
	subject, err := templateToText(emailSubjectTemplate, args)
	if err != nil {
		return err
	}
	html, err := templateToHTML(emailHTMLTemplate, args)
	if err != nil {
		return err
	}

	e := &email.Email{
		To: args.To,
		// From:    fmt.Sprintf("Release Bot <%s>", smtpOpts.user),
		From:    smtpOpts.from,
		Subject: subject,
		Text:    []byte(text),
		HTML:    []byte(html),
		Headers: textproto.MIMEHeader{},
	}
	emailAuth := smtp.PlainAuth("", smtpOpts.user, smtpOpts.password, smtpOpts.host)
	addr := fmt.Sprintf("%s:%d", smtpOpts.host, smtpOpts.port)
	return e.Send(addr, emailAuth)
}
