package com.github.mmolimar.kukulcan.repl;

import jdk.internal.jshell.tool.JShellToolBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Entry point for the Kukulcan REPL with the JShell.
 *
 */
public class JKukulcanRepl {

    public static void main(String[] args) throws Exception {
        Map<String, String> prefs = new HashMap<>();
        prefs.put("STARTUP",
                "System.out.println(com.github.mmolimar.kukulcan.repl.package$.MODULE$.banner());\n" +
                        "import com.github.mmolimar.kukulcan.java.Kukulcan;");

        JShellToolBuilder jShellToolBuilder = new JShellToolBuilder();
        jShellToolBuilder.persistence(prefs);

        String[] predefs = {"--feedback", "concise"};
        String[] jshellArgs = Stream.of(predefs, args).flatMap(Stream::of).toArray(String[]::new);
        jShellToolBuilder.start(jshellArgs);
    }
}
