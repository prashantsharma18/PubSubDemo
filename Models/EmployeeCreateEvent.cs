using System;
using System.Collections.Generic;
using System.Text;

namespace Models
{
    public class EmployeeCreateEvent : IntegrationEvent
    {
        public EmployeeCreateEvent(string empid, string desig, string handler, Guid id, DateTime createDate) : base(id, createDate)
        {
            EmpId = empid;
            Designation = desig;
            EvenHandler = handler;
        }

        public string EmpId { get; set; }
        public string Designation { get; set; }
        public string EvenHandler { get; set; }

    }
}
