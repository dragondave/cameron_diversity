function getElementsByStyleClass (className) {
  var all = document.all ? document.all :
    document.getElementsByTagName('*');
  var elements = new Array();
  for (var e = 0; e < all.length; e++)
    if (all[e].className == className)
      elements[elements.length] = all[e];
  return elements;
}

function hideOnLoad() {

	elements = getElementsByStyleClass("expandable");

	for(i=0;i<elements.length;i++){

		elements[i].style.display = "none";

	}

}

function cookie_create(name,value,days) {
	if (days) {
		var date = new Date();
		date.setTime(date.getTime()+(days*24*60*60*1000));
		var expires = "; expires="+date.toGMTString();
	}
	else var expires = "";
	document.cookie = name+"="+value+expires+"; path=/";
}

function cookie_read(name) {
	var nameEQ = name + "=";
	var ca = document.cookie.split(';');
	for(var i=0;i < ca.length;i++) {
		var c = ca[i];
		while (c.charAt(0)==' ') c = c.substring(1,c.length);
		if (c.indexOf(nameEQ) == 0) return c.substring(nameEQ.length,c.length);
	}
	return null;
}

function cookie_erase(name) {
	createCookie(name,"",-1);
}

function accessibility_initFontSize() {

  //alert("Function called");

  if(cookie_read("hesaAccessibilityTextSizePref")){

    var theSize = cookie_read("hesaAccessibilityTextSizePref");

    //alert("Cookie exists.  Read size = "+theSize);

    var setStr = theSize+"px";

    document.getElementById('bodyTag').style.fontSize = setStr;

    //alert("After attempt to change, body font size is now "+document.getElementById('bodyTag').style.fontSize);

    //alert("from cookie: "+document.getElementById('bodyTag').style.fontSize);

  }else{

    //alert("Cookie not found.  Body font size right now: "+document.getElementById('bodyTag').style.fontSize);

    document.getElementById('bodyTag').style.fontSize = "12px";

    //alert("Font size setting attempt made.  Font size now: "+document.getElementById('bodyTag').style.fontSize);

    cookie_create("hesaAccessibilityTextSizePref", 12, 1);

    //alert("Cookie created.");

    //alert("set for first time: "+document.getElementById('bodyTag').style.fontSize);

  }

}

function outputScriptResizingMarkup() {

  	document.write('<a title = \"Decrease font size\" href = \"javascript:nowt();\" onclick = \"accessibility_adjustFontSize(-1, false)\">');
        document.write('A-');
        document.write('<\/a>');
  	document.write('<span style = \"color: #333;\"> :: :: <\/span>');
  	document.write('<a title = \"Reset font size\"  href = \"javascript:nowt();\" onclick = \"accessibility_adjustFontSize(12, true);\">');
        document.write('A0');
        document.write('<\/a>');
  	document.write('<span style = \"color: #333;\"> :: :: <\/span>');
  	document.write('<a title = \"Increase font size\"  href = \"javascript:nowt();\" onclick = \"accessibility_adjustFontSize(1, false);\">');
        document.write('A+');
        document.write('<\/a>');
  	document.write('<span style = \"color: #333;\"> :: :: <\/span>	');

}

function accessibility_adjustFontSize(thisMuch, reset) {

  var sizeNow = parseInt(document.getElementById('bodyTag').style.fontSize);

  if(reset){

	//set limits
	if(thisMuch > 30){

		thisMuch = 30;

	}

	if(thisMuch < 8) {

		thisMuch = 8;

	}

    document.getElementById('bodyTag').style.fontSize = thisMuch + "px";
    //document.getElementById('hesaBanner').innerHTML = "&nbsp;";

  }else{

	//set limits
	if(sizeNow > 30 && thisMuch > 0) {

		thisMuch = 0;

	}

	if(sizeNow < 8 && thisMuch < 0) {

		thisMuch = 0;

	}

    document.getElementById('bodyTag').style.fontSize = (sizeNow + thisMuch) + "px";
    //document.getElementById('hesaBanner').innerHTML = "&nbsp;";

  }

  cookie_create("hesaAccessibilityTextSizePref", parseInt(document.getElementById('bodyTag').style.fontSize), 1);

}

function nowt() {

  //function that does very little for use in hrefs that need to go nowhere but validate OK.
  //I understand that most people call it void()
  var zilch = 0;

}