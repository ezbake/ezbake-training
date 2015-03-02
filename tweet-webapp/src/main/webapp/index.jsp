<%@ page language="java" contentType="text/html; charset=ISO-8859-1" pageEncoding="ISO-8859-1" %>
<%@ page import="ezbake.configuration.EzConfiguration,ezbake.security.client.EzbakeSecurityClient" %>
<!--  
 * Copyright (C) 2013-2014 Computer Sciences Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. -->
<html>
<head>

    <!-- Bootstrap -->
    <link href="css/bootstrap.css" rel="stylesheet">
    <!--<link href="css/bootstrap.css.map" rel="stylesheet">-->
    <!--<link href="css/bootstrap-theme.css.map" rel="stylesheet">-->
    <link href="css/bootstrap-theme.css" rel="stylesheet">

    <!-- HTML5 Shim and Respond.js IE8 support of HTML5 elements and media queries -->
    <!-- WARNING: Respond.js doesn't work if you view the page via file:// -->
    <!--[if lt IE 9]>
    <script src="https://oss.maxcdn.com/libs/html5shiv/3.7.0/html5shiv.js"></script>
    <script src="https://oss.maxcdn.com/libs/respond.js/1.4.2/respond.min.js"></script>
    <![endif]-->

    <script src="jquery-1.10.2.js"></script>

    <script type="text/javascript">
        $(function () {
            $('#tweetContent').keyup(function () {
                var count = $(this).val().length;
                $('#characters').text(count);
                //console.log("# char: " + count);
            });

            $("#searchText").keyup(function (event) {
                //console.log("searchText keyup: " + event.keyCode);
                if (event.keyCode == 13) {
                    $("#searchButton").click();
                }
            });

            $("#formalVisibility").focusout(function (event) {
                var formalVisibility = $("#formalVisibility").val();

                if (formalVisibility.trim().length > 0) {
                    $.ajax({
                        url: 'MongoDbServlet', type: "POST", data: {
                            action: 'validateVisibility', formalVisibility: formalVisibility
                        }, success: function (result) {
                            $("#formalVisibilityValidation").attr('class', 'text-success');
                            $("#formalVisibilityValidation").html("Valid");
                        }, error: function (xhr, status, result) {
                            var err = xhr.responseText;
                            console.log("ERROR in formal visibility validation: " + err);
                            $("#formalVisibilityValidation").attr('class', 'text-danger');
                            $("#formalVisibilityValidation").html(err);
                        }
                    });
                } else {
                    $("#formalVisibilityValidation").html("");
                }

            });
        });

        function insertTweet() {
            if (!validateNumCharacters()) {
                return;
            }

            var userName = $("#userName").val();
            var formalVisibility = $("#formalVisibility").val();
            var tweetContent = $("#tweetContent").val();

            $("#insertResult").attr('class', 'text-success');
            $("#insertResult").html("Calling the Content Publisher service, please wait...");
            $("#insertButton").prop("disabled", true);

            $.ajax({
                url: 'MongoDbServlet', type: "POST", data: {
                    action: 'insertTweet',
                    userName: userName,
                    formalVisibility: formalVisibility,
                    tweetContent: tweetContent
                }, success: function (result) {
                    $("#insertResult").attr('class', 'text-success');
                    $("#insertResult").html(result);
                    $("#insertButton").prop("disabled", false);
                }, error: function (xhr, status, result) {
                    var err = xhr.responseText;
                    console.log("ERROR in insert: " + err);
                    $("#insertResult").attr('class', 'text-danger');
                    $("#insertResult").html(err);
                    $("#insertButton").prop("disabled", false);
                }
            });
        }


        function searchTweet() {
            var searchText = $("#searchText").val();

            $.ajax({
                url: 'MongoDbServlet', type: "POST", data: {
                    action: 'searchTweet', searchText: searchText
                }, success: function (result) {
                    $("#searchResult tbody").html(result);
                }, error: function (xhr, status, result) {
                    var err = xhr.responseText;
                    console.log("ERROR in search: " + err);
                    $("#searchResult tbody").html(err);
                }
            });

            $.ajax({
                url: 'MongoDbServlet', type: "POST", data: {
                    action: 'getWordCount', searchText: searchText
                }, success: function (result) {
                    $("#wordCount").html(result);
                }, error: function (xhr, status, result) {
                    var err = xhr.responseText;
                    console.log("ERROR in getWordCount: " + err);
                    $("#wordCount").html(err);
                }
            });
        }

        function validateNumCharacters() {
            var count = $('#tweetContent').val().length;

            if (count > 140) {
                alert("You have exceeded 140 characters: " + count);
                return false;
            }

            return true;
        }
    </script>
</head>
<body>

<div id="auths" style="background-color: gold">
    <% try {
        EzbakeSecurityClient securityClient = new EzbakeSecurityClient(new EzConfiguration().getProperties());
        out.println(securityClient.fetchTokenForProxiedUser().getTokenPrincipal().getPrincipal());
        out.println(securityClient.fetchTokenForProxiedUser().getAuthorizations());
    } catch (Exception e) {
        e.printStackTrace();
    }%>
</div>
<div id="warning" style="background-color: red; text-align:center; color:white">
    Please note that the security level visibilities in this app are all test data - not real classified data.
</div>

<div id="container">
    <div class="panel panel-default" id="content">
        <h3 class="panel-title">Tweet Webapp Sample</h3>

        <!-- Insert Tweet -->
        <div class="panel-body">
            <label>Insert Tweet</label>

            <div class="left-inner-addon">
                <i class="glyphicon glyphicon-user"></i>
                <input type="text" id="userName" class="form-control" placeholder="User Name"></br>
            </div>
            <em class="hint" style="margin-top: 10px;">Formal visibility for tweet (for example: A&B&(C|D)):</em>
            <em id="formalVisibilityValidation">

            </em>
            <input type="text" id="formalVisibility" class="form-control" placeholder=""></br>
            <em class="hint" style="margin-top: 10px;">Tweet:</em>
            <textarea id="tweetContent" class="form-control" rows="4" cols="40"
                      placeholder="Enter tweet message here"></textarea>
            <em class="hint" style="margin-top: 10px;">Number of characters:
                <div id="characters" class="numCharacters">0</div>
            </em>
            <em class="hint" style="margin-top: 10px;">(max 140 characters)</em>
            <span class="input-group-btn">
                <button class="btn btn-info btn-large" id="insertButton" type="button" name="insert"
                        onClick="insertTweet()">Submit Tweet
                </button>
            </span>

            <!-- Insert Tweet output msg -->
            <div id="insertResult" class="text-success"></div>
        </div>

        <!-- Search Tweet -->
        <ul class="list-group">
            <li class="list-group-item">
                <label>Search Tweet</label>
                <em class="hint" style="margin-top: 10px;">Enter search term (stop words such as 'the', 'an', 'a' are
                    not supported):</em>
                <input id="searchText" type="text" class="form-control" placeholder=""></br>

            <span class="input-group-btn">
                <button class="btn btn-info btn-large" id="searchButton" type="button" name="search"
                        onClick="searchTweet()">Search Tweet
                </button>
            </span>
            </li>
            <li class="list-group-item">
                Word count (the number of times the search word appears in the tweets):
                <div id="wordCount">
                    0
                </div>
            </li>
            <li class="list-group-item">
                <!-- Search result output list -->
                <table id="searchResult" class="table table-condensed">
                    <thead>
                        <tr>
                            <th class="_id">_id</th>
                            <th class="id">id</th>
                            <th class="classification">formal visibility (classification)</th>
                            <th class="tweet">user: tweet</th>
                        </tr>
                    </thead>
                    <tbody>
                        <!-- empty on initial page load. Will get populated by search results using AJAX. -->
                    </tbody>
                </table>
            </li>
        </ul>
    </div>
</div>

<!-- jQuery (necessary for Bootstrap's JavaScript plugins) -->
<script src="https://ajax.googleapis.com/ajax/libs/jquery/1.11.0/jquery.min.js"></script>
<!-- Include all compiled plugins (below), or include individual files as needed -->
<script src="js/bootstrap.min.js"></script>
</body>
</html>
