function trim_timestamp(datetimestamp) {

    // Given json datetime format can have different granularity (-> minutes/seconds/microseconds)
    // between sources this truncates to a consistent timestamp for our needs going as far as minutes

    // Loads of ways to handle dates and times but just need to match intervals so keeping it simple for now

    return datetimestamp.split(":", 2).join(":").replace("z", "").replace("Z", "");
}

function uk_address_to_region(address) {

    // NationalGrid accepts Outcode from Postcode to identify regions

    // Using regex filter for UK Postcode definition as per wikipedia for now:
    // https://en.wikipedia.org/wiki/Postcodes_in_the_United_Kingdom#Validation

    ukpostcode_regex = '([Gg][Ii][Rr] 0[Aa]{2})|((([A-Za-z][0-9]{1,2})|(([A-Za-z][A-Ha-hJ-Yj-y][0-9]{1,2})|(([A-Za-z][0-9][A-Za-z])|([A-Za-z][A-Ha-hJ-Yj-y][0-9][A-Za-z]?))))\\s?[0-9][A-Za-z]{2})';
    postcode = address.match(ukpostcode_regex);

    // Sorting to pull back the shortest match which should be the outcode/first section
    postcode.sort(function (a, b) {
        return a.length - b.length;
    });
    return postcode[0];
}

function percent(smallnumber, bignumber, rounding = 2) {
    return Number((smallnumber / bignumber) * 100).toFixed(2);
}

function logstamp() {
    var console_log = console.log;
    var timeStart = new Date().getTime();

    return function () {
        var delta = new Date().getTime() - timeStart;
        var args = [];
        var datestamp = new Date().toISOString().replace(/T/, " ").replace(/\..+/, "");

        args.push("[" + datestamp + "] ");
        for (var i = 0; i < arguments.length; i++) {
            args.push(arguments[i]);
        }
        console_log.apply(console, args);
    };
}

module.exports = { trim_timestamp, uk_address_to_region, logstamp, percent };