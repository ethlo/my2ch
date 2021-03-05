package my2ch;

import org.springframework.validation.annotation.Validated;

import com.ethlo.my2ch.config.Schedule;
import com.ethlo.my2ch.config.Source;
import com.ethlo.my2ch.config.Target;
import com.ethlo.my2ch.config.TransferConfig;

@Validated
public class ValidatedTransferConfig extends TransferConfig
{
    public ValidatedTransferConfig(final String alias, final Schedule schedule, final Source source, final Target target)
    {
        super(alias, schedule, source, target);
    }
}
