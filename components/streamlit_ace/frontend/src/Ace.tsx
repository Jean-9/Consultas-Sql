import { useEffect, useRef, useState } from "react"
import {
  ComponentProps,
  Streamlit,
  withStreamlitConnection,
  Theme,
} from "streamlit-component-lib"
import AceEditor from "react-ace"
import { IAceEditor } from "react-ace/lib/types"
import { Paper, Button, Grid } from "@material-ui/core"
import { MuiThemeProvider, createTheme } from "@material-ui/core/styles"

import "ace-builds/webpack-resolver"
import "ace-builds/src-min-noconflict/ext-emmet"
import "ace-builds/src-min-noconflict/ext-language_tools"

import "./index.css" // Aqui está o estilo visual para deixar o texto em maiúsculo

interface AceProps extends ComponentProps {
  args: any
  theme?: Theme
}

const Ace = ({ args, theme }: AceProps) => {
  const [colors, setColors] = useState<any>({})
  const [changed, setChanged] = useState<boolean>(false)
  const editorRef = useRef<IAceEditor>(null)
  const debounceRef = useRef<number>(0)

  let timeout: NodeJS.Timeout

  // Send editor content to streamlit
  const updateStreamlit = (value: string) => {
    Streamlit.setComponentValue(value.toUpperCase()) // força o valor em maiúsculo
    setChanged(false)
  }

  // Called on editor update
  const handleChange = (value: string) => {
    clearTimeout(timeout)

    timeout = setTimeout(() => {
      if (args.autoUpdate) {
        updateStreamlit(value)
      }
      else {
        setChanged(true)
      }
    }, 2000) // debounceRef.current)
  }

  // Update content keybinding
  useEffect(() => {
  if (!editorRef.current) return

  const editor = editorRef.current.editor

  editor.commands.removeCommand("addLineAfter")
  editor.commands.addCommand({
    name: "updateStreamlit",
    bindKey: { mac: "cmd-return", win: "ctrl-return" },
    exec: (editor: IAceEditor) => {
      if (args.autoUpdate) {
        editor.selection.clearSelection()
        editor.navigateLineEnd()
        editor.insert("\n")
      } else if (changed) {
        updateStreamlit(editor.getValue())
      }
    },
  })

  debounceRef.current = args.autoUpdate ? 200 : 0

  // 🔧 Setup custom autocomplete
  if (args.completer && Array.isArray(args.completer)) {
    const customCompleter = {
  getCompletions: function (editor: any, session: any, pos: any, prefix: any, callback: any) {
  const line = session.getLine(pos.row)
  const cursorColumn = pos.column
  const textBeforeCursor = line.substring(0, cursorColumn)

  // Extrai a última "palavra" válida (para prefixo real)
  const match = textBeforeCursor.match(/(\w+)$/)
  const lastWord = (match ? match[1] : "").toUpperCase()

  const results = args.completer
    .filter((item: any) => {
      const value = (item.value || "").toUpperCase()
      return value.startsWith(lastWord)
    })
    .map((item: any) => ({
      value: item.value,
      caption: item.value,
      meta: item.meta || "custom"
    }))

  callback(null, results)
}
}

    // Sobrescreve todos os completers com apenas o nosso
    editor.completers = [customCompleter]
  }

  // 🔠 Força o texto digitado em maiúsculo


}, [])  // <- fim do useEffect

  // Update theme
  useEffect(() => {
    setColors({
      palette: {
        primary: {
          main: theme?.primaryColor,
          background: {
            default: theme?.backgroundColor,
          },
          text: {
            primary: theme?.textColor,
          }
        }
      }
    })
  }, [theme?.primaryColor, theme?.backgroundColor, theme?.textColor])


  // Set default prop values that shouldn't be exposed to python
  args.enableBasicAutocompletion = true
  args.enableLiveAutocompletion = true
  args.enableSnippets = true
  args.onChange = handleChange
  args.width = "100%"

  // Auto height
  if (!args.height) {
    args.maxLines = Infinity
  }

  const resizeObserver = new ResizeObserver((entries: any) => {
    Streamlit.setFrameHeight(entries[0].contentRect.height + 15)
  })

  const observeElement = (element: HTMLDivElement | null) => {
    if (element !== null)
      resizeObserver.observe(element)
    else
      resizeObserver.disconnect()
  }

  return (
    <div ref={observeElement}>
      <MuiThemeProvider theme={createTheme(colors)}>
        <Paper>
          <AceEditor ref={editorRef} {...args} />
        </Paper>

      </MuiThemeProvider>
    </div>
  )
}

export default withStreamlitConnection(Ace)