function getval(sel) {
    // $('#queryParams').val(sel.value);
    if (sel.value == "-1") {
        $('#patients').css({ display: "none" });
        $('#admissions').css({ display: "none" });
        $('#icu').css({ display: "none" });
        $('#services').css({ display: "none" });
        $('#prescriptions').css({ display: "none" });
    } else if (sel.value == "1") {
        $('#patients').css({ display: "" });
        $('#admissions').css({ display: "none" });
        $('#icu').css({ display: "none" });
        $('#services').css({ display: "none" });
        $('#prescriptions').css({ display: "none" });
    } else if (sel.value == "2") {
        $('#patients').css({ display: "none" });
        $('#admissions').css({ display: "" });
        $('#icu').css({ display: "none" });
        $('#services').css({ display: "none" });
        $('#prescriptions').css({ display: "none" });
    } else if (sel.value == "3") {
        $('#patients').css({ display: "none" });
        $('#admissions').css({ display: "none" });
        $('#icu').css({ display: "" });
        $('#services').css({ display: "none" });
        $('#prescriptions').css({ display: "none" });
    } else if (sel.value == "4") {
        $('#patients').css({ display: "none" });
        $('#admissions').css({ display: "none" });
        $('#icu').css({ display: "none" });
        $('#services').css({ display: "" });
        $('#prescriptions').css({ display: "none" });
    } else if (sel.value == "5") {
        $('#patients').css({ display: "none" });
        $('#admissions').css({ display: "none" });
        $('#icu').css({ display: "none" });
        $('#services').css({ display: "none" });
        $('#prescriptions').css({ display: "" });
    }
    // $('#' + $(this).val()).show();
}

function changeFilterAnalytics(sel) {
    if (sel.value == "-1") {
        $('#one').css({ display: "none" });
        $('#two').css({ display: "none" });
        $('#three').css({ display: "none" });
    } else if (sel.value == "6") {
        $('#one').css({ display: "" });
        $('#two').css({ display: "none" });
        $('#three').css({ display: "none" });
    } else if (sel.value == "7") {
        $('#one').css({ display: "none" });
        $('#two').css({ display: "" });
        $('#three').css({ display: "none" });
    } else if (sel.value == "8") {
        $('#one').css({ display: "none" });
        $('#two').css({ display: "none" });
        $('#three').css({ display: "" });
    }
}


let parts = {}
$( document ).ready(function() {
    $('#dataTable').append(
        `<thead id="searchTableHead">
        </thead>
        <tfoot id="searchTableFoot">
        </tfoot>
        <tbody id="searchTableBody">
        </tbody>`
    )
    $("#analyticsSubmit").click(async function() {
        // alert( "ready!" );
        $('#paritionDropdown').children().remove().end().append("<option value='all_data'>All data</option>");
        queryNumber = $("#queryNumber").find(":selected").val()
        let postBody = {
            "server": 1,
            "query": queryNumber,
            "options": {}
        }
        
        if (queryNumber == "6") {
            if ($('#language').val() != "-1") {
                postBody["options"]["language"] = $('#language').val();
            }
            if ($('#discharge_location').val() != "-1") {
                postBody["options"]["discharge_location"] = $('#discharge_location').val();
            }
            if ($('#ethnicity').val()!="-1") {
                postBody["options"]["ethnicity"] = $('#ethnicity').val();
            }
        } else if (queryNumber == "7") {
            if ($('#intime').val() != "-1") {
                postBody["options"]["intime"] = $('#intime').val();
            }
            if ($('#outtime').val() != "-1") {
                postBody["options"]["outtime"] = $('#outtime').val();
            }
        } else if (queryNumber == "8") {
            if ($('#prescription_date').val() != "-1") {
                postBody["options"]["prescription_date"] = $('#prescription_date').val();
            }
            if ($('#form_unit_disp').val() != "-1") {
                postBody["options"]["form_unit_disp"] = $('#form_unit_disp').val();
            }
            if ($('#formulary_drug_cd').val()!="-1") {
                postBody["options"]["formulary_drug_cd"] = $('#formulary_drug_cd').val();
            }
        }
        // cmdQuery = $("#commandText").val()
        try {
            const response = await fetch('http://127.0.0.1:5000/task2', {
                method: 'post',
                // mode: 'cors',
                body: JSON.stringify(postBody)
            });
            res = await response.json()
            console.log('Completed!', res);
            let partitions = res.partitions;
            $.each(partitions, function(k, v) {   
                let key = Object.keys(v)[0];
                let value = v[key];
                parts[key] = value;
                console.log("key val", key, value);
                $('#paritionDropdown')
                    .append($("<option></option>")
                                .attr("value", key)
                                .text("Partition " + key)); 
            });
            parts["all_data"] = res["all_data"];
            if (queryNumber == "7") {
                $('#queryOutput').text("Before: " + res["all_data"][0] + " After: " + res["all_data"][1]);    
            } else if (queryNumber == "8") {
                $('#queryOutput').text("Female: " + res["all_data"]['F'][0] + "->" + res["all_data"]['F'][1] + " Male: " + res["all_data"]['M'][0] + "->" + res["all_data"]['M'][1]);    
            } else {
                $('#queryOutput').text(res["all_data"]);
            }

        } catch (err) {
            console.error(`Error: ${err}`);
        }
    });

    $("#searchSubmit").click(async function() {
        // alert( "ready!" );
        $('#searchParitionDropdown').children().remove().end().append("<option value='all_data'>All data</option>");
        $('#searchTableHead').children().remove().end()
        $('#searchTableFoot').children().remove().end()
        $('#searchTableBody').children().remove().end()

        queryNumber = $("#query").find(":selected").val()
        let postBody = {
            "server": 1,
            "query": queryNumber,
            "options": {}
        }
        if (queryNumber == "1") {
            if ($('#dod').val() != "-1") {
                postBody["options"]["dod"] = $('#dod').val();
            }
            if ($('#expire_flag').val() != "-1") {
                postBody["options"]["expire_flag"] = parseInt($('#expire_flag').val());
            }
        } else if (queryNumber == "2") {
            if ($('#marital_status').val() != "-1") {
                postBody["options"]["marital_status"] = $('#marital_status').val();
            }
            if ($('#insurance').val() != "-1") {
                postBody["options"]["insurance"] = $('#insurance').val();
            }
            if ($('#language').val()!="-1") {
                postBody["options"]["language"] = $('#language').val();
            }
        } else if (queryNumber == "3") {
            if ($('#intime').val() != "-1") {
                postBody["options"]["intime"] = $('#intime').val();
            }
            if ($('#outtime').val() != "-1") {
                postBody["options"]["outtime"] = $('#outtime').val();
            }
        } else if (queryNumber == "4") {
            if ($('#transfertime').val() != "-1") {
                postBody["options"]["transfertime"] = $('#transfertime').val();
            }
        } else if (queryNumber == "5") {
            if ($('#prescription_date').val() != "-1") {
                postBody["options"]["prescription_date"] = $('#prescription_date').val();
            }
            if ($('#form_unit_disp').val() != "-1") {
                postBody["options"]["form_unit_disp"] = $('#form_unit_disp').val();
            }
            if ($('#formulary_drug_cd').val() != "-1") {
                postBody["options"]["formulary_drug_cd"] = $('#formulary_drug_cd').val();
            }
        }
        
        
        // cmdQuery = $("#commandText").val()

        const response = await fetch('http://127.0.0.1:5000/task2', {
            method: 'post',
            // mode: 'cors',
            body: JSON.stringify(postBody)
        });
        res = await response.json()
        console.log('Completed!', res);
        let partitions = res.partitions;
        $.each(partitions, function(k, v) {   
            let key = Object.keys(v)[0];
            let value = v[key];
            parts[key] = value;
            console.log("key val", key, value);
            $('#searchParitionDropdown')
                .append($("<option></option>")
                            .attr("value", key)
                            .text("Partition " + key)); 
        });
        parts["all_data"] = res["all_data"];
        
        $('#searchTableHead').append(`<tr>`);
        $('#searchTableFoot').append(`<tr>`);
        if (res["all_data"].length > 0 ) {

        
            for (let [key, value] of Object.entries(res["all_data"][0])) {
                $('#searchTableHead').append(`
                        <th>${key}</th>
                `);
                $('#searchTableFoot').append(`
                        <th>${key}</th>
                `);
                console.log(`${key}: ${value}`);
            }
        }
        $('#searchTableHead').append(`</tr>`);
        $('#searchTableFoot').append(`</tr>`);

        $('#numberOfEntries').text(res["all_data"].length + " Entries")

        res["all_data"].forEach(element => {
            $('#searchTableBody').append(`<tr>`);
            for (let [key, value] of Object.entries(element)) {
                $('#searchTableBody').append(`<td>${value}</td>`);
            }
            $('#searchTableBody').append(`</tr>`);
        });
        
        // if (queryNumber == "7") {
        //     $('#queryOutput').text("Before: " + res["all_data"][0] + " After: " + res["all_data"][1]);    
        // } else if (queryNumber == "8") {
        //     $('#queryOutput').text("Female: " + res["all_data"]['F'][0] + "->" + res["all_data"]['F'][1] + " Male: " + res["all_data"]['M'][0] + "->" + res["all_data"]['M'][1]);    
        // } else {
        //     $('#queryOutput').text(res["all_data"]);
        // }


    });
});

function analyticsOpSelect(sel) {
    console.log(parts, sel.value);
    
    if (queryNumber == "7") {
        $('#queryOutput').text("Before: " + parts[sel.value][0] + " After: " + parts[sel.value][1]);    

    } else if (queryNumber == "8") {
        $('#queryOutput').text("Female: " + parts[sel.value]['F'][0] + "->" + parts[sel.value]['F'][1] + " Male: " + parts[sel.value]['M'][0] + "->" + parts[sel.value]['M'][1]);    
    } else {
        $('#queryOutput').text(parts[sel.value]);
    }
}   

function searchOpSelect(sel) {
    console.log(parts, sel.value);
    data = parts[sel.value]

    $('#searchTableHead').children().remove().end()
    $('#searchTableFoot').children().remove().end()
    $('#searchTableBody').children().remove().end()

    $('#searchTableHead').append(`<tr>`);
    $('#searchTableFoot').append(`<tr>`);
    if (res["all_data"].length > 0 ) {
        for (let [key, value] of Object.entries(data[0])) {
            $('#searchTableHead').append(`
                    <th>${key}</th>
            `);
            $('#searchTableFoot').append(`
                    <th>${key}</th>
            `);
            console.log(`${key}: ${value}`);
        }
    }
    $('#searchTableHead').append(`</tr>`);
    $('#searchTableFoot').append(`</tr>`);

    $('#numberOfEntries').text(data.length + " Entries")

    data.forEach(element => {
        $('#searchTableBody').append(`<tr>`);
        for (let [key, value] of Object.entries(element)) {
            $('#searchTableBody').append(`<td>${value}</td>`);
        }
        $('#searchTableBody').append(`</tr>`);
    });
    
    // if (queryNumber == "7") {
    //     $('#queryOutput').text("Before: " + parts[sel.value][0] + " After: " + parts[sel.value][1]);    

    // } else if (queryNumber == "8") {
    //     $('#queryOutput').text("Female: " + parts[sel.value]['F'][0] + "->" + parts[sel.value]['F'][1] + " Male: " + parts[sel.value]['M'][0] + "->" + parts[sel.value]['M'][1]);    
    // } else {
    //     $('#queryOutput').text(parts[sel.value]);
    // }
}