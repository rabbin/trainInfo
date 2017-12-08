system = require('system')  
address = system.args[1];
var page = require('webpage').create();  
var url = address;  
page.open(url, function (status) {  
    //Page is loaded!  
    if (status !== 'success') {  
        console.log('Unable to post!');  
    } else {  
        window.setTimeout(function () {
            page.render("C:\\Users\\rabbin\\Desktop\\spark\\rabbin\\conf\\test1.png");
            console.log(page.content);
            phantom.exit();
        }, 5000);   
    } 
});
