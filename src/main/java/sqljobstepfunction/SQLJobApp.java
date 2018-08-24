/*
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
 * limitations under the License.
 */

package sqljobstepfunction;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.*;
import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.amazonaws.services.stepfunctions.AWSStepFunctionsClientBuilder;
import com.amazonaws.services.stepfunctions.model.GetActivityTaskRequest;
import com.amazonaws.services.stepfunctions.model.GetActivityTaskResult;
import com.amazonaws.services.stepfunctions.model.SendTaskFailureRequest;
import com.amazonaws.services.stepfunctions.model.SendTaskSuccessRequest;
import com.amazonaws.util.json.Jackson;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.concurrent.TimeUnit;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class SQLJobApp {

	public static void main(final String[] args) throws Exception {

		String connectionUrl = "jdbc:sqlserver://<database endpoint >;databaseName=CustomerDB;user=AWSUser;password=<your password>";
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		String result = null;

		ClientConfiguration clientConfiguration = new ClientConfiguration();
		clientConfiguration.setSocketTimeout((int) TimeUnit.SECONDS.toMillis(70));

		AWSStepFunctions client = AWSStepFunctionsClientBuilder.standard().withRegion(Regions.US_EAST_1)
				.withCredentials(new DefaultAWSCredentialsProviderChain()).withClientConfiguration(clientConfiguration)
				.build();

		while (true) {
			GetActivityTaskResult getActivityTaskResult = client
					.getActivityTask(new GetActivityTaskRequest().withActivityArn("<Enter ACTIVITY_ARN>"));

			if (getActivityTaskResult.getTaskToken() != null) {
				try {
					JsonNode json = Jackson.jsonNodeOf(getActivityTaskResult.getInput());
					String command = json.get("Command").textValue();

					if (command.equals("start"))
						try {

							Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
							con = DriverManager.getConnection(connectionUrl);

							String SQL = "EXEC [dbo].[NumOfCustDelay]";
							stmt = con.createStatement();
							rs = stmt.executeQuery(SQL);

							while (rs.next()) {

								System.out.println(rs.getString("NumberOfCustomers"));
								result = rs.getString("NumberOfCustomers");
							}

						} catch (Exception e) {
							e.printStackTrace();
						} finally {

							if (rs != null)
								try {
									rs.close();
								} catch (Exception e) {
									e.printStackTrace();
								}
							if (stmt != null)
								try {
									stmt.close();
								} catch (Exception e) {
									e.printStackTrace();
								}
							if (con != null)
								try {
									con.close();
								} catch (Exception e) {
									e.printStackTrace();
								}

						}
					else
						result = "fail";

					client.sendTaskSuccess(new SendTaskSuccessRequest().withOutput(result)
							.withTaskToken(getActivityTaskResult.getTaskToken()));
				} catch (Exception e) {
					client.sendTaskFailure(
							new SendTaskFailureRequest().withTaskToken(getActivityTaskResult.getTaskToken()));
				}
			} else {
				Thread.sleep(1000);
			}
		}
	}

}
