package org.astraea.argument;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import java.time.Duration;
import java.util.Set;

/*
 * A tool used to parse command line arguments.
 *
 * To add new option, add new option in the corresponding file.
 * @Parameter(names={"--option"}, description="")
 * public <optionType> <optionName>;
 * */

public class ArgumentUtil {
  // Do not instantiate.
  private ArgumentUtil() {}

  /**
   * Side effect: parse args into toolArgument
   * @param toolArgument An argument object that the user want.
   * @param args Command line arguments that are put into main function.
   */
  public static void parseArgument(Object toolArgument, String[] args) {
    JCommander jc = JCommander.newBuilder().addObject(toolArgument).build();
    try {
      jc.parse(args);
    } catch (ParameterException pe) {
      jc.usage();
      throw pe;
    }
  }

  /* Validate Classes */
  public static class NotEmpty implements IParameterValidator {
    @Override
    public void validate(String name, String value) throws ParameterException {
      if (value == null || value.equals(""))
        throw new ParameterException(name + " should not be empty.");
    }
  }

  public static class LongPositive implements IParameterValidator {
    @Override
    public void validate(String name, String value) throws ParameterException {
      if (Long.parseLong(value) <= 0) throw new ParameterException(name + " should be positive.");
    }
  }

  public static class LongNotNegative implements IParameterValidator {
    @Override
    public void validate(String name, String value) throws ParameterException {
      if (Long.parseLong(value) < 0)
        throw new ParameterException(name + " should not be negative.");
    }
  }

  /* Converter classes */
  public static class DurationConverter implements IStringConverter<Duration> {
    @Override
    public Duration convert(String value) {
      return Duration.ofSeconds(Long.parseLong(value));
    }
  }

  public static class SetConverter implements IStringConverter<Set<String>> {
    @Override
    public Set<String> convert(String value) {
      String[] values = value.split(",");
      return Set.of(values);
    }
  }
}
