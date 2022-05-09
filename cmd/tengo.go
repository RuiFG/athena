package main

import (
	"bufio"
	"fmt"
	"github.com/d5/tengo/v2"
	"github.com/d5/tengo/v2/parser"
	"github.com/d5/tengo/v2/stdlib"
	"github.com/spf13/cobra"
	"io"
	"os"
)

func init() {
	Command.AddCommand(&cobra.Command{
		Use:   "tengo",
		Short: "run REPL",
		Long:  `run tengo REPL for test`,
		Run: func(cmd *cobra.Command, args []string) {
			modules := stdlib.GetModuleMap(stdlib.AllModuleNames()...)
			RunREPL(modules, os.Stdin, os.Stdout)
		},
	})
}

const (
	replPrompt = ">> "
)

// RunREPL starts REPL.
func RunREPL(modules *tengo.ModuleMap, in io.Reader, out io.Writer) {
	stdin := bufio.NewScanner(in)
	fileSet := parser.NewFileSet()
	globals := make([]tengo.Object, tengo.GlobalsSize)
	symbolTable := tengo.NewSymbolTable()
	for idx, fn := range tengo.GetAllBuiltinFunctions() {
		symbolTable.DefineBuiltin(idx, fn.Name)
	}

	// embed println function
	symbol := symbolTable.Define("__repl_println__")
	globals[symbol.Index] = &tengo.UserFunction{
		Name: "println",
		Value: func(args ...tengo.Object) (ret tengo.Object, err error) {
			var printArgs []interface{}
			for _, arg := range args {
				if _, isUndefined := arg.(*tengo.Undefined); isUndefined {
					printArgs = append(printArgs, "<undefined>")
				} else {
					s, _ := tengo.ToString(arg)
					printArgs = append(printArgs, s)
				}
			}
			printArgs = append(printArgs, "\n")
			_, _ = fmt.Print(printArgs...)
			return
		},
	}

	var constants []tengo.Object
	for {
		_, _ = fmt.Fprint(out, replPrompt)
		scanned := stdin.Scan()
		if !scanned {
			return
		}

		line := stdin.Text()
		srcFile := fileSet.AddFile("repl", -1, len(line))
		p := parser.NewParser(srcFile, []byte(line), nil)
		file, err := p.ParseFile()
		if err != nil {
			_, _ = fmt.Fprintln(out, err.Error())
			continue
		}

		file = addPrints(file)
		c := tengo.NewCompiler(srcFile, symbolTable, constants, modules, nil)
		if err := c.Compile(file); err != nil {
			_, _ = fmt.Fprintln(out, err.Error())
			continue
		}

		bytecode := c.Bytecode()
		machine := tengo.NewVM(bytecode, globals, -1)
		if err := machine.Run(); err != nil {
			_, _ = fmt.Fprintln(out, err.Error())
			continue
		}
		constants = bytecode.Constants
	}
}

func addPrints(file *parser.File) *parser.File {
	var stmts []parser.Stmt
	for _, s := range file.Stmts {
		switch s := s.(type) {
		case *parser.ExprStmt:
			stmts = append(stmts, &parser.ExprStmt{
				Expr: &parser.CallExpr{
					Func: &parser.Ident{Name: "__repl_println__"},
					Args: []parser.Expr{s.Expr},
				},
			})
		case *parser.AssignStmt:
			stmts = append(stmts, s)

			stmts = append(stmts, &parser.ExprStmt{
				Expr: &parser.CallExpr{
					Func: &parser.Ident{
						Name: "__repl_println__",
					},
					Args: s.LHS,
				},
			})
		default:
			stmts = append(stmts, s)
		}
	}
	return &parser.File{
		InputFile: file.InputFile,
		Stmts:     stmts,
	}
}
