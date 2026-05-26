// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"
	htmltemplate "html/template"
	"log"
	"net/smtp"
	"net/textproto"
	"path/filepath"

	"github.com/jordan-wright/email"
)

const (
	templatePrefixUpdateVersions = "update-versions"
)

type messageDataUpdateVersions struct {
	Version string
	PRs     []htmltemplate.URL
}

type message struct {
	Subject  string
	TextBody string
	HTMLBody string
}

type messageTemplates struct {
	SubjectPrefix string
	BodyPrefixes  []string
}

// newMessage generates new message parts, based on:
// - templatePrefix - the filename prefix for subject/txt/html templates in the ./templates/ folder
// - data - the data object applied to the html/text/subject templates
func newMessage(
	templatesDir string, templates messageTemplates, data interface{},
) (*message, error) {
	subject, err := templateFilesToText(data, filepath.Join(templatesDir, templates.SubjectPrefix+".subject"))
	if err != nil {
		return nil, fmt.Errorf("templateToText %s: %w", templates.SubjectPrefix+".subject", err)
	}
	var textTemplateFiles, htmlTemplateFiles []string
	for _, prefix := range templates.BodyPrefixes {
		textTemplateFiles = append(textTemplateFiles, filepath.Join(templatesDir, prefix+".txt"))
		htmlTemplateFiles = append(htmlTemplateFiles, filepath.Join(templatesDir, prefix+".gohtml"))
	}
	text, err := templateFilesToText(data, textTemplateFiles...)
	if err != nil {
		return nil, fmt.Errorf("templateToText %s: %w", templates.BodyPrefixes, err)
	}
	html, err := templateFilesToHTML(data, htmlTemplateFiles...)
	if err != nil {
		return nil, fmt.Errorf("templateToHTML %s: %w", templates.BodyPrefixes, err)
	}
	return &message{
		Subject:  subject,
		TextBody: text,
		HTMLBody: html,
	}, nil
}

type sendOpts struct {
	templatesDir string
	host         string
	port         int
	user         string
	password     string
	from         string
	to           []string
}

func sendMailUpdateVersions(args messageDataUpdateVersions, opts sendOpts) error {
	template := messageTemplates{
		SubjectPrefix: templatePrefixUpdateVersions,
		BodyPrefixes:  []string{templatePrefixUpdateVersions},
	}
	msg, err := newMessage(opts.templatesDir, template, args)
	if err != nil {
		return fmt.Errorf("newMessage: %w", err)
	}
	log.Printf("dry-run: sendMailUpdateVersions:\n")
	log.Printf("Subject: %s\n\n%s\n", msg.Subject, msg.TextBody)
	return sendmail(msg, opts)
}

// sendmail creates and sends an email to the releases mailing list.
// sendmail is specified as a function closure to allow for testing
// of sendMail* methods.
var sendmail = func(content *message, smtpOpts sendOpts) error {
	e := &email.Email{
		To:      smtpOpts.to,
		From:    smtpOpts.from,
		Subject: content.Subject,
		Text:    []byte(content.TextBody),
		HTML:    []byte(content.HTMLBody),
		Headers: textproto.MIMEHeader{},
	}
	emailAuth := smtp.PlainAuth("", smtpOpts.user, smtpOpts.password, smtpOpts.host)
	addr := fmt.Sprintf("%s:%d", smtpOpts.host, smtpOpts.port)
	if err := e.Send(addr, emailAuth); err != nil {
		return fmt.Errorf("cannot send email: %w", err)
	}
	return nil
}
