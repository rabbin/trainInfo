system = require('system')
address = system.args[1];//获得命令行第二个参数 接下来会用到
var page = require('webpage').create();
var url = address;
page.open(url, function (status) {
    if (status !== 'success') {
        console.log('Unable to post!');
        phantom.exit();
    } else {
        window.setTimeout(function () {
            console.log(page.content);
            phantom.exit();
        },5000)
    }
});
