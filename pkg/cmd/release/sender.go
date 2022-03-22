// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"fmt"
	htmltemplate "html/template"
	"net/smtp"
	"net/textproto"
	"os"
	"path/filepath"
	"strings"

	"github.com/jordan-wright/email"
)

const (
	templatePrefixPickSHA           = "pick-sha"
	templatePrefixPostBlockers      = "post-blockers"
	templatePrefixPostBlockersAlpha = "post-blockers.alpha"
)

type messageDataPickSHA struct {
	Version          string
	SHA              string
	TrackingIssue    string
	TrackingIssueURL htmltemplate.URL
	DiffURL          htmltemplate.URL
}

// ProjectBlocker lists the number of blockers per project.
// This needs to be public in order to be used by the html/template engine.
type ProjectBlocker struct {
	ProjectName string
	NumBlockers int
}

type messageDataPostBlockers struct {
	Version       string
	PrepDate      string
	ReleaseDate   string
	TotalBlockers int
	BlockersURL   string
	ReleaseBranch string
	BlockerList   []ProjectBlocker
}

type postBlockerTemplateArgs struct {
	BackportsUseBackboard       bool
	BackportsWeeklyTriageReview bool
}

type message struct {
	Subject  string
	TextBody string
	HTMLBody string
}

func loadTemplate(templatesDir, template string) (string, error) {
	file, err := os.ReadFile(filepath.Join(templatesDir, template))
	if err != nil {
		return "", fmt.Errorf("loadTemplate %s: %w", template, err)
	}
	return string(file), nil
}

// newMessage generates new message parts, based on:
// - templatePrefix - the filename prefix for subject/txt/html templates in the ./templates/ folder
// - data - the data object applied to the html/text/subject templates
func newMessage(templatesDir string, templatePrefix string, data interface{}) (*message, error) {
	subjectTemplate, err := loadTemplate(templatesDir, templatePrefix+".subject")
	if err != nil {
		return nil, err
	}
	subject, err := templateToText(subjectTemplate, data)
	if err != nil {
		return nil, fmt.Errorf("templateToText %s: %w", templatePrefix+".subject", err)
	}

	textTemplate, err := loadTemplate(templatesDir, templatePrefix+".txt")
	if err != nil {
		return nil, err
	}
	text, err := templateToText(textTemplate, data)
	if err != nil {
		return nil, fmt.Errorf("templateToText %s: %w", templatePrefix+".txt", err)
	}

	htmlTemplate, err := loadTemplate(templatesDir, templatePrefix+".gohtml")
	if err != nil {
		return nil, err
	}
	html, err := templateToHTML(htmlTemplate, data)
	if err != nil {
		return nil, fmt.Errorf("templateToHTML %s: %w", templatePrefix+".gohtml", err)
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

func sendMailPostBlockers(args messageDataPostBlockers, opts sendOpts) error {
	templatePrefix := templatePrefixPostBlockers

	// Backport policy:
	// - stable/production: refer to backboard
	// - alpha: no need to mention backports
	// - beta/rc's: backports reviewed during triage meeting
	backportsUseBackboard := false
	backportsWeeklyTriageReview := false

	switch {
	case strings.Contains(args.Version, "-alpha."):
		// alpha copy is so different that we'll use a completely separate template.
		templatePrefix = templatePrefixPostBlockersAlpha
	case
		strings.Contains(args.Version, "-beta."),
		strings.Contains(args.Version, "-rc."):
		backportsWeeklyTriageReview = true
	default: // stable/production
		backportsUseBackboard = true
	}

	data := struct {
		Args     messageDataPostBlockers
		Template postBlockerTemplateArgs
	}{
		Args: args,
		Template: postBlockerTemplateArgs{
			BackportsUseBackboard:       backportsUseBackboard,
			BackportsWeeklyTriageReview: backportsWeeklyTriageReview,
		},
	}
	msg, err := newMessage(opts.templatesDir, templatePrefix, data)
	if err != nil {
		return fmt.Errorf("newMessage: %w", err)
	}
	return sendmail(msg, opts)
}

func sendMailPickSHA(args messageDataPickSHA, opts sendOpts) error {
	msg, err := newMessage(opts.templatesDir, templatePrefixPickSHA, args)
	if err != nil {
		return fmt.Errorf("newMessage: %w", err)
	}
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
