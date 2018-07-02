package de.julielab.concepts.db.application;


import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.OptionDef;
import org.kohsuke.args4j.spi.Messages;
import org.kohsuke.args4j.spi.OptionHandler;
import org.kohsuke.args4j.spi.Parameters;
import org.kohsuke.args4j.spi.Setter;

/**
 * Works just like {@link org.kohsuke.args4j.spi.StringArrayOptionHandler} with the exception that the original handler
 * returns a null value if no arguments are given to the parameter. It is this not determinable if the parameter was
 * given but without arguments or not given at all. This handler will add the empty string in case
 * that no arguments are given but the parameter is still present.
 * IMPORTANT: This means that you will have to check for empty argument values when using this handler.
 */
public class OptionalStringArrayOptionHandler extends OptionHandler<String> {

    public OptionalStringArrayOptionHandler(CmdLineParser parser, OptionDef option, Setter<String> setter) {
        super(parser, option, setter);
    }

    /**
     * Returns {@code "STRING[]"}.
     *
     * @return return "STRING[]";
     */
    @Override
    public String getDefaultMetaVariable() {
        return Messages.DEFAULT_META_STRING_ARRAY_OPTION_HANDLER.format();
    }

    /**
     * Tries to parse {@code String[]} argument from {@link Parameters}.
     */
    @Override
    public int parseArguments(Parameters params) throws CmdLineException {
        int counter = 0;
        boolean gotElement = false;
        for (; counter < params.size(); counter++) {
            String param = params.getParameter(counter);

            if (param.startsWith("-")) {
                break;
            }

            for (String p : param.split(" ")) {
                gotElement = true;
                setter.addValue(p);
            }
        }
        if (!gotElement)
            setter.addValue("");

        return counter;
    }

}