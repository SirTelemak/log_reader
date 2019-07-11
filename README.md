# Log reader
Read all log files in given directory and sorts it by `valid` or `non_valid`, then by day and then counts number of event types
## Usage

```
python reader.py --directory [log directory] --output [output file] --processors [quantity]
```
### Available commands

`--directory, -d` - path to log directory;

`--output, -o` - path to output file;

`--processors, -p` - number of processors to be used;

`--help` - view help.


## Authors

* **Sergey Teplyakov** - *Initial work* - [Telemak](https://github.com/SirTelemak)
