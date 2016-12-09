package com.skt.sql.utils;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.NoViableAltException;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.apache.commons.lang.StringUtils;

import java.sql.SQLException;

public class SQLErrorListener extends BaseErrorListener
{
  public void syntaxError(Recognizer<?, ?> recognizer,
                          Object offendingSymbol,
                          int line, int charPositionInLine,
                          String msg,
                          RecognitionException e)
  {
    CommonTokenStream tokens = (CommonTokenStream) recognizer.getInputStream();
    String input = tokens.getTokenSource().getInputStream().toString();
    Token token = (Token) offendingSymbol;
    String[] lines = StringUtils.splitPreserveAllTokens(input, '\n');
    String errorLine = lines[line - 1];

    String simpleMessage = "syntax error at or near \"" + token.getText() + "\"";
    String extendedMessage = null;
    if (e instanceof NoViableAltException) {
      if (token.getText().equals("<EOF>")) {
        extendedMessage = "suggest to add identifiers";
      } else {
        extendedMessage = "suggest to delete \"" + token.getText() + "\"";
      }
    } else if (e == null) {
      if (msg != null) {
        extendedMessage = "possible alternatives: [" + msg.substring(msg.indexOf("{") + 1, msg.indexOf("}")) + "]";
      }
    }
    throw new RuntimeException(makeMessage(token, line, charPositionInLine, simpleMessage, extendedMessage, errorLine));
  }

  private String makeMessage(Token token, int line, int charPositionInLine, String header, String tail, String errorLine) {
    StringBuilder sb = new StringBuilder();
    int displayLimit = 80;
    String queryPrefix = "LINE " + line + ":" + " ";
    String prefixPadding = StringUtils.repeat(" ", queryPrefix.length());
    String locationString;

    int tokenLength = token.getStopIndex() - token.getStartIndex() + 1;
    if(tokenLength > 0){
      locationString = StringUtils.repeat(" ", charPositionInLine) + StringUtils.repeat("^", tokenLength);
    } else {
      locationString = StringUtils.repeat(" ", charPositionInLine) + "^";
    }

    sb.append(header).append("\n");
    sb.append(queryPrefix);

    if (errorLine.length() > displayLimit) {
      int padding = (displayLimit / 2);

      String ellipsis = " ... ";
      int startPos = locationString.length() - padding - 1;
      if (startPos <= 0) {
        startPos = 0;
        sb.append(errorLine.substring(startPos, displayLimit)).append(ellipsis).append("\n");
        sb.append(prefixPadding).append(locationString);
      } else if (errorLine.length() - (locationString.length() + padding) <= 0) {
        startPos = errorLine.length() - displayLimit - 1;
        sb.append(ellipsis).append(errorLine.substring(startPos)).append("\n");
        sb.append(prefixPadding).append(StringUtils.repeat(" ", ellipsis.length()))
            .append(locationString.substring(startPos));
      } else {
        sb.append(ellipsis).append(errorLine.substring(startPos, startPos + displayLimit)).append(ellipsis).append("\n");
        sb.append(prefixPadding).append(StringUtils.repeat(" ", ellipsis.length()))
            .append(locationString.substring(startPos));
      }
    } else {
      sb.append(errorLine).append("\n");
      sb.append(prefixPadding).append(locationString);
    }
    if (tail!= null) {
      sb.append("\n").append(tail);
    }
    return sb.toString();
  }
}
