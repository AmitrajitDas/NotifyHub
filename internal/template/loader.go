package template

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"

	"github.com/amitrajitdas31/notifyhub/internal/domain"
)

// TemplateFile represents a parsed template file with front-matter.
type TemplateFile struct {
	Name            string
	Channel         domain.Channel
	SubjectTemplate *string
	BodyTemplate    string
}

// LoadFromDir reads all .tmpl files from dir and parses them.
// Returns a list of domain templates ready for persistence.
func LoadFromDir(dir string) ([]domain.Template, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read dir %s: %w", dir, err)
	}

	var templates []domain.Template
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !strings.HasSuffix(entry.Name(), ".tmpl") {
			continue
		}

		path := filepath.Join(dir, entry.Name())
		parsed, err := parseFile(path)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", path, err)
		}

		tmpl := domain.Template{
			ID:                uuid.New(),
			Name:              parsed.Name,
			Channel:           parsed.Channel,
			SubjectTemplate:   parsed.SubjectTemplate,
			BodyTemplate:      parsed.BodyTemplate,
			Version:           1,
		}
		templates = append(templates, tmpl)
	}

	return templates, nil
}

// parseFile reads a .tmpl file and extracts front-matter + body.
// Front-matter is YAML-like: key: value pairs between --- markers.
func parseFile(path string) (*TemplateFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	content := string(data)
	if !strings.HasPrefix(content, "---\n") {
		return nil, fmt.Errorf("template must start with ---")
	}

	idx := strings.Index(content[4:], "---\n")
	if idx < 0 {
		return nil, fmt.Errorf("missing closing --- in front-matter")
	}

	frontMatter := content[4 : 4+idx]
	body := strings.TrimSpace(content[4+idx+4:])

	if body == "" {
		return nil, fmt.Errorf("template body is empty")
	}

	parsed := &TemplateFile{BodyTemplate: body}
	for _, line := range strings.Split(frontMatter, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])

		switch key {
		case "name":
			parsed.Name = val
		case "channel":
			parsed.Channel = domain.Channel(val)
		case "subject":
			parsed.SubjectTemplate = &val
		}
	}

	if parsed.Name == "" {
		return nil, fmt.Errorf("template name is required in front-matter")
	}
	if !parsed.Channel.IsValid() {
		return nil, fmt.Errorf("invalid channel %q in front-matter", parsed.Channel)
	}

	return parsed, nil
}
