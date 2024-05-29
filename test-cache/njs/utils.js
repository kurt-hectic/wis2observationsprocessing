function version(r) {
    r.return(200, njs.version);
}

function myversion(r) {

    let randomValue = Math.floor(Math. random()*100) + 1

    r.return(200, "my" + njs.version + " " + randomValue);
}

function randomfail(r) {

    let fail_rate = parseFloat(process.env.FAIL_RATE);
    let randomValue = Math.random();

    if (randomValue < fail_rate) {
       return "fail";
    }
    else {
        return "ok";
    }

}



export default {version, myversion, randomfail}
