package utilities;

/**
 * <p>Title: </p>
 * <p>Description: </p>
 * <p>Copyright: Copyright (c) 2004</p>
 * <p>Company: </p>
 * @author not attributable
 * @version 1.0
 */

import java.awt.event.KeyEvent;
import javax.swing.JTextField;


public class IntegerTextField extends JTextField {
  int m;
  public IntegerTextField(int n) {m=n;}

    final static String badchars
        = "-`~!@#$%^&*()_+=\\|\"':;?/>.<, ";

    public void processKeyEvent(KeyEvent ev) {

      char c = ev.getKeyChar();

      if ( (Character.isLetter(c) && !ev.isAltDown())
          || badchars.indexOf(c) > -1 ||
          (this.getText().length()>= m && c != '\b')) {
        ev.consume();
        return;
      }
      if (c == '-' && getDocument().getLength() > 0)
        ev.consume();
      else
        super.processKeyEvent(ev);


  }
}
