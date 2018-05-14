package webui

import (
	"fmt"
	"strings"

	"github.com/tebeka/selenium"
)

type NavBarLink struct {
	wd     selenium.WebDriver
	elem   selenium.WebElement
	target Page
}

func (link NavBarLink) IsActive() bool {
	cls, err := link.elem.GetAttribute("class")
	if err != nil {
		// TODO(couchand): not this.
		panic(fmt.Sprintf("unable to find navbar! %s", err))
	}

	return strings.Contains(cls, "active")
}

func (link NavBarLink) Click() Page {
	err := link.elem.Click()
	if err != nil {
		// TODO(couchand): not this.
		panic(fmt.Sprintf("unable to find navbar! %s", err))
	}

	return link.target
}

type NavBar struct {
	wd   selenium.WebDriver
	elem selenium.WebElement
}

func (bar NavBar) OverviewLink() NavBarLink {
	elem, err := bar.elem.FindElement(selenium.ByCSSSelector, `a[href="#/overview"]`)
	if err != nil {
		// TODO(couchand): not this.
		panic(fmt.Sprintf("unable to find navbar link! %s", err))
	}

	return NavBarLink{
		wd:     bar.wd,
		elem:   elem,
		target: MakeOverviewPage(bar.wd),
	}
}

func (bar NavBar) MetricsLink() NavBarLink {
	elem, err := bar.elem.FindElement(selenium.ByCSSSelector, `a[href="#/metrics"]`)
	if err != nil {
		// TODO(couchand): not this.
		panic(fmt.Sprintf("unable to find navbar link! %s", err))
	}

	return NavBarLink{
		wd:     bar.wd,
		elem:   elem,
		target: MakeMetricsPage(bar.wd),
	}
}

func (bar NavBar) DatabasesLink() NavBarLink {
	elem, err := bar.elem.FindElement(selenium.ByCSSSelector, `a[href="#/databases"]`)
	if err != nil {
		// TODO(couchand): not this.
		panic(fmt.Sprintf("unable to find navbar link! %s", err))
	}

	return NavBarLink{
		wd:     bar.wd,
		elem:   elem,
		target: MakeDatabasesPage(bar.wd),
	}
}

func (bar NavBar) JobsLink() NavBarLink {
	elem, err := bar.elem.FindElement(selenium.ByCSSSelector, `a[href="#/jobs"]`)
	if err != nil {
		// TODO(couchand): not this.
		panic(fmt.Sprintf("unable to find navbar link! %s", err))
	}

	return NavBarLink{
		wd:     bar.wd,
		elem:   elem,
		target: MakeJobsPage(bar.wd),
	}
}

type Page interface {
	NavBar() NavBar
	Title() string
	Heading() string
}

type BasePage struct {
	wd selenium.WebDriver
}

func (page BasePage) NavBar() NavBar {
	elem, err := page.wd.FindElement(selenium.ByCSSSelector, "nav.navigation-bar")
	if err != nil {
		// TODO(couchand): not this.
		panic(fmt.Sprintf("unable to find navbar! %s", err))
	}

	return NavBar{
		wd:   page.wd,
		elem: elem,
	}
}

func (page BasePage) Title() string {
	title, err := page.wd.Title()
	if err != nil {
		// TODO(couchand): not this.
		panic(fmt.Sprintf("unable to find navbar! %s", err))
	}

	return title
}

func (page BasePage) Heading() string {
	elem, err := page.wd.FindElement(selenium.ByCSSSelector, "h1")
	if err != nil {
		// TODO(couchand): not this.
		panic(fmt.Sprintf("unable to find navbar! %s", err))
	}

	heading, err := elem.Text()
	if err != nil {
		// TODO(couchand): not this.
		panic(fmt.Sprintf("unable to find navbar! %s", err))
	}

	return heading
}

type OverviewPage struct {
	BasePage
}

var _ Page = OverviewPage{}

func MakeOverviewPage(wd selenium.WebDriver) OverviewPage {
	return OverviewPage{BasePage{wd}}
}

type MetricsPage struct {
	BasePage
}

var _ Page = MetricsPage{}

func MakeMetricsPage(wd selenium.WebDriver) MetricsPage {
	return MetricsPage{BasePage{wd}}
}

type DatabasesPage struct {
	BasePage
}

var _ Page = DatabasesPage{}

func MakeDatabasesPage(wd selenium.WebDriver) DatabasesPage {
	return DatabasesPage{BasePage{wd}}
}

type JobsPage struct {
	BasePage
}

var _ Page = JobsPage{}

func MakeJobsPage(wd selenium.WebDriver) JobsPage {
	return JobsPage{BasePage{wd}}
}
