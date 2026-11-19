package internal_test

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const module = "github.com/niicsz/binitech-sms/internal/"

type rule struct {
	layer  string
	denied []string
}

var rules = []rule{
	{layer: "domain", denied: []string{module + "application", module + "adapter", module + "platform", module + "contract"}},
	{layer: "contract", denied: []string{module + "application", module + "adapter", module + "platform"}},
	{layer: "application", denied: []string{module + "adapter", module + "platform"}},
	{layer: "adapter/in", denied: []string{module + "adapter/out", module + "platform"}},
	{layer: "adapter/out", denied: []string{module + "adapter/in", module + "application/usecase"}},
}

var infrastructure = []string{
	"github.com/jackc/", "github.com/twmb/", "github.com/go-chi/",
	"github.com/prometheus/", "github.com/redis/", "net/http", "database/sql",
}

func TestHexagonalBoundaries(t *testing.T) {
	for _, r := range rules {
		for file, imports := range importsOf(t, r.layer) {
			for _, imp := range imports {
				for _, denied := range r.denied {
					if strings.HasPrefix(imp, denied) {
						t.Errorf("%s importa %s: a camada %s não pode depender de %s", file, imp, r.layer, denied)
					}
				}
			}
		}
	}
}

func TestCoreHasNoInfrastructure(t *testing.T) {
	for _, layer := range []string{"domain", "contract", "application"} {
		for file, imports := range importsOf(t, layer) {
			for _, imp := range imports {
				for _, lib := range infrastructure {
					if strings.HasPrefix(imp, lib) {
						t.Errorf("%s importa %s: o núcleo não conhece infraestrutura", file, imp)
					}
				}
			}
		}
	}
}

func importsOf(t *testing.T, layer string) map[string][]string {
	t.Helper()
	root := filepath.FromSlash(layer)
	result := map[string][]string{}
	if _, err := os.Stat(root); err != nil {
		t.Fatalf("camada %s não encontrada: %v", layer, err)
	}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, spec := range file.Imports {
			value, _ := strconv.Unquote(spec.Path.Value)
			result[path] = append(result[path], value)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("lendo %s: %v", layer, err)
	}
	return result
}
